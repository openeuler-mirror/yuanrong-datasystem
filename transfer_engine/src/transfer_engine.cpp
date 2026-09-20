#include "datasystem/transfer_engine/transfer_engine.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <limits>
#include <map>
#include <random>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "internal/connection/connection_manager.h"
#include "internal/control_plane/control_plane.h"
#include "internal/control_plane/transfer_control_service.h"
#include "internal/log/logging.h"
#include "internal/log/environment_dump.h"
#include "internal/runtime/acl_runtime_helper.h"
#ifdef TRANSFER_ENGINE_ENABLE_HIXL
#include "internal/backend/ascend/ascend_backend.h"
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
constexpr uint64_t K_FINALIZE_LEASE_WAIT_TIMEOUT_MS = 30000;
constexpr int32_t K_MAX_HIXL_READ_ATTEMPTS = 2;
constexpr size_t K_MAX_BATCH_READ_ITEMS = 4096;
constexpr size_t K_MAX_BATCH_REGISTRATION_ITEMS = 4096;
constexpr uint64_t K_MAX_TCP_PORT = 65535;
constexpr uint64_t K_DECIMAL_BASE = 10;
// ']' and ':' separate the bracketed IPv6 host from the port in "[host]:port".
constexpr size_t K_IPV6_PORT_SEPARATOR_LEN = 2;
constexpr int32_t K_RPC_PORT_RANGE_BIND_ATTEMPTS = 500;
constexpr int K_MIN_CONFIGURABLE_RPC_PORT = 1024;
constexpr int K_EPHEMERAL_PORT_START = 32768;
constexpr int K_EPHEMERAL_PORT_END = 60999;

struct RpcPortRange {
    bool enabled = false;
    int minPort = 0;
    int maxPort = 0;
};

bool IsConfigurableRpcPort(int port)
{
    return port >= K_MIN_CONFIGURABLE_RPC_PORT && port <= static_cast<int>(K_MAX_TCP_PORT) &&
        !(port >= K_EPHEMERAL_PORT_START && port <= K_EPHEMERAL_PORT_END);
}

// Parses a strictly positive decimal integer; rejects any non-digit character, overflow, and zero
// so that misspelled values (for example "25000abc") never take effect silently.
bool ParseRpcPortEnv(const char *env, int *port)
{
    int64_t value = 0;
    for (const char *p = env; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') {
            return false;
        }
        value = value * K_DECIMAL_BASE + static_cast<int64_t>(*p - '0');
        if (value > std::numeric_limits<int>::max()) {
            return false;
        }
    }
    if (value <= 0) {
        return false;
    }
    *port = static_cast<int>(value);
    return true;
}

// YR_TE_RPC_PORT_MIN/YR_TE_RPC_PORT_MAX constrain OS-assigned (port 0) RPC ports to a fixed range.
// They mirror Mooncake's MC_MIN_RPC_PORT/MC_MAX_RPC_PORT: unset means OS assignment, and any invalid
// or partial configuration warns and keeps OS assignment.
RpcPortRange ReadRpcPortRangeFromEnv()
{
    const char *minEnv = std::getenv("YR_TE_RPC_PORT_MIN");
    const char *maxEnv = std::getenv("YR_TE_RPC_PORT_MAX");
    if (minEnv == nullptr && maxEnv == nullptr) {
        return RpcPortRange{};
    }
    auto fallback = [](const std::string &reason) {
        TE_LOG_WARNING << "ignoring YR_TE_RPC_PORT_MIN/YR_TE_RPC_PORT_MAX configuration (" << reason <<
                       "); using an OS-assigned RPC port";
        return RpcPortRange{};
    };
    if (minEnv == nullptr || maxEnv == nullptr) {
        return fallback("both variables must be set together");
    }
    if (minEnv[0] == '\0' || maxEnv[0] == '\0') {
        return fallback("both variables must be non-empty");
    }
    int minPort = 0;
    int maxPort = 0;
    if (!ParseRpcPortEnv(minEnv, &minPort) || !ParseRpcPortEnv(maxEnv, &maxPort)) {
        return fallback("both variables must be decimal integers");
    }
    if (!IsConfigurableRpcPort(minPort) || !IsConfigurableRpcPort(maxPort)) {
        return fallback("ports must be in 1024-65535 and outside the ephemeral range 32768-60999");
    }
    if (minPort > maxPort) {
        return fallback("min port exceeds max port");
    }
    return RpcPortRange{ true, minPort, maxPort };
}

std::string ToLowerAscii(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
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

Result ValidateProtocol(const std::string &protocol)
{
    const std::string protocolLower = ToLowerAscii(protocol);
    TE_CHECK_OR_RETURN(protocolLower == "ascend", ErrorCode::kInvalid,
                       "transfer engine protocol only supports ascend");
    return Result::OK();
}

Result CreateAscendBackend([[maybe_unused]] std::shared_ptr<IDataPlaneBackend> &backend)
{
#ifdef TRANSFER_ENGINE_ENABLE_HIXL
    backend = std::make_shared<AscendBackend>();
    return Result::OK();
#else
    return TE_MAKE_STATUS(ErrorCode::kNotSupported,
                          "ascend backend is not compiled; configure with -DTRANSFER_ENGINE_ENABLE_HIXL=ON");
#endif
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

Result ParseTargetHostname(const std::string &targetHostname, std::string *peerHost, uint16_t *peerPort,
                           bool allowZeroPort = false)
{
    TE_CHECK_PTR_OR_RETURN(peerHost);
    TE_CHECK_PTR_OR_RETURN(peerPort);
    TE_CHECK_OR_RETURN(!targetHostname.empty(), ErrorCode::kInvalid, "targetHostname is empty");

    std::string host;
    std::string portStr;

    if (targetHostname.front() == '[') {
        const size_t close = targetHostname.find(']');
        TE_CHECK_OR_RETURN(close != std::string::npos, ErrorCode::kInvalid, "invalid targetHostname");
        TE_CHECK_OR_RETURN(close + K_IPV6_PORT_SEPARATOR_LEN <= targetHostname.size()
                               && targetHostname[close + 1] == ':',
                           ErrorCode::kInvalid, "targetHostname missing port");
        host = targetHostname.substr(1, close - 1);
        portStr = targetHostname.substr(close + K_IPV6_PORT_SEPARATOR_LEN);
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
    TE_CHECK_OR_RETURN(allowZeroPort || portValue > 0, ErrorCode::kInvalid,
                       "targetHostname port should be positive");

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
    TE_CHECK_OR_RETURN(deviceName.compare(0, kDevicePrefixLen, kDevicePrefix) == 0, ErrorCode::kInvalid,
                       "device_name should match npu:${device_id}");

    int64_t parsedDeviceId = 0;
    for (size_t i = kDevicePrefixLen; i < deviceName.size(); ++i) {
        const char c = deviceName[i];
        TE_CHECK_OR_RETURN(c >= '0' && c <= '9', ErrorCode::kInvalid, "device_name should match npu:${device_id}");
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

    struct BackingEntry {
        uint64_t addr = 0;
        uint64_t length = 0;
        size_t logicalRefCount = 0;
    };

    std::unordered_map<std::string, EndpointCacheEntry> endpointOwnerDeviceCache;
    // TransferEngine accesses backingEntries only while holding apiMutex_.
    std::vector<BackingEntry> backingEntries;
};

namespace {

bool IsRangeOverlap(uint64_t leftAddr, uint64_t leftLength, uint64_t rightAddr, uint64_t rightLength)
{
    return !(leftAddr + leftLength <= rightAddr || rightAddr + rightLength <= leftAddr);
}

bool IsSameBacking(const TransferEngineState::BackingEntry &entry, uint64_t addr, uint64_t length)
{
    return entry.addr == addr && entry.length == length;
}

bool IsRetryableHixlReadFailure(const Result &result)
{
    return result.GetCode() == ErrorCode::kNotReady || result.GetCode() == ErrorCode::kRuntimeError;
}

using BackingEntries = std::vector<TransferEngineState::BackingEntry>;

std::vector<TransferEngineState::BackingEntry>::iterator FindBackingEntry(BackingEntries *backingEntries,
                                                                          uint64_t backingAddr, uint64_t backingLength)
{
    return std::find_if(backingEntries->begin(), backingEntries->end(),
                        [backingAddr, backingLength](const TransferEngineState::BackingEntry &item) {
                            return IsSameBacking(item, backingAddr, backingLength);
                        });
}

using BackingRangeIndex = std::map<uint64_t, uint64_t>;

Result CheckExistingBackingUniqueOrFail(const BackingEntries &entries, uint64_t backingAddr, uint64_t backingLength,
                                        bool *exactExists)
{
    *exactExists = false;
    for (const auto &entry : entries) {
        if (IsSameBacking(entry, backingAddr, backingLength)) {
            *exactExists = true;
            return Result::OK();
        }
        TE_CHECK_OR_RETURN(!IsRangeOverlap(entry.addr, entry.length, backingAddr, backingLength), ErrorCode::kInvalid,
                           "backing memory ranges overlap without being identical");
    }
    return Result::OK();
}

Result CheckBackingUniqueOrFail(const BackingRangeIndex &entries, uint64_t backingAddr, uint64_t backingLength,
                                bool *exactExists)
{
    *exactExists = false;
    const auto next = entries.lower_bound(backingAddr);
    if (next != entries.end() && next->first == backingAddr) {
        if (next->second == backingLength) {
            *exactExists = true;
            return Result::OK();
        }
        return TE_MAKE_STATUS(ErrorCode::kInvalid, "backing memory ranges overlap without being identical");
    }
    if (next != entries.begin()) {
        auto previous = next;
        --previous;
        TE_CHECK_OR_RETURN(!IsRangeOverlap(previous->first, previous->second, backingAddr, backingLength),
                           ErrorCode::kInvalid, "backing memory ranges overlap without being identical");
    }
    TE_CHECK_OR_RETURN(next == entries.end() ||
                           !IsRangeOverlap(next->first, next->second, backingAddr, backingLength),
                       ErrorCode::kInvalid, "backing memory ranges overlap without being identical");
    return Result::OK();
}

Result PlanBatchRegistration(const std::vector<MemoryRegistration> &registrations, int32_t deviceId,
                             const BackingEntries &existingBackings, std::vector<RegisteredRegion> *logicalRegions,
                             BackingEntries *newBackings)
{
    BackingRangeIndex newBackingIndex;

    logicalRegions->reserve(registrations.size());
    for (const auto &registration : registrations) {
        const uint64_t logicalAddr = static_cast<uint64_t>(registration.logicalAddr);
        const uint64_t logicalLength = static_cast<uint64_t>(registration.logicalLength);
        const uint64_t backingAddr = static_cast<uint64_t>(registration.backingAddr);
        const uint64_t backingLength = static_cast<uint64_t>(registration.backingLength);
        TE_CHECK_OR_RETURN(logicalAddr > 0 && logicalLength > 0, ErrorCode::kInvalid,
                           "logical memory range should be positive");
        TE_CHECK_OR_RETURN(backingAddr > 0 && backingLength > 0, ErrorCode::kInvalid,
                           "backing memory range should be positive");
        TE_CHECK_OR_RETURN(logicalAddr <= std::numeric_limits<uint64_t>::max() - logicalLength, ErrorCode::kInvalid,
                           "logical memory range overflow");
        TE_CHECK_OR_RETURN(backingAddr <= std::numeric_limits<uint64_t>::max() - backingLength, ErrorCode::kInvalid,
                           "backing memory range overflow");
        TE_CHECK_OR_RETURN(logicalAddr >= backingAddr && logicalAddr + logicalLength <= backingAddr + backingLength,
                           ErrorCode::kInvalid, "logical memory range is outside its backing range");
        logicalRegions->push_back(RegisteredRegion{ logicalAddr, logicalLength, deviceId, backingAddr, backingLength });

        bool exactBackingExists = false;
        TE_RETURN_IF_ERROR(
            CheckExistingBackingUniqueOrFail(existingBackings, backingAddr, backingLength, &exactBackingExists));
        if (exactBackingExists) {
            continue;
        }
        bool plannedExactBacking = false;
        TE_RETURN_IF_ERROR(CheckBackingUniqueOrFail(newBackingIndex, backingAddr, backingLength, &plannedExactBacking));
        if (!plannedExactBacking) {
            newBackingIndex.emplace(backingAddr, backingLength);
            newBackings->push_back(TransferEngineState::BackingEntry{ backingAddr, backingLength, 0 });
        }
    }
    return Result::OK();
}

void CommitBackings(BackingEntries *backingEntries, const BackingEntries &newBackings,
                    const std::vector<RegisteredRegion> &logicalRegions)
{
    backingEntries->insert(backingEntries->end(), newBackings.begin(), newBackings.end());
    for (const auto &region : logicalRegions) {
        auto entry = FindBackingEntry(backingEntries, region.backingBaseAddr, region.backingLength);
        if (entry != backingEntries->end()) {
            ++entry->logicalRefCount;
        }
    }
}

bool UnregisterAllBackings(IDataPlaneBackend *backend, const BackingEntries &backings)
{
    bool rollbackOk = true;
    for (auto iter = backings.rbegin(); iter != backings.rend(); ++iter) {
        rollbackOk = backend->UnregisterLocalMemory(iter->addr, iter->length).IsOk() && rollbackOk;
    }
    return rollbackOk;
}

bool ReregisterAllBackings(IDataPlaneBackend *backend, const BackingEntries &backings)
{
    bool rollbackOk = true;
    for (auto iter = backings.rbegin(); iter != backings.rend(); ++iter) {
        rollbackOk = backend->RegisterLocalMemory(iter->addr, iter->length).IsOk() && rollbackOk;
    }
    return rollbackOk;
}

Result PlanBatchUnregister(const std::vector<uintptr_t> &bufferAddrs, RegisteredMemoryTable *registeredMemory,
                           BackingEntries *backingEntries, std::vector<uint64_t> *baseAddrs,
                           std::vector<RegisteredRegion> *regions)
{
    std::unordered_set<uint64_t> uniqueBaseAddrs;
    baseAddrs->reserve(bufferAddrs.size());
    regions->reserve(bufferAddrs.size());
    for (const auto bufferAddr : bufferAddrs) {
        TE_CHECK_OR_RETURN(bufferAddr > 0, ErrorCode::kInvalid, "bufferAddr should be positive");
        const uint64_t baseAddr = static_cast<uint64_t>(bufferAddr);
        TE_CHECK_OR_RETURN(uniqueBaseAddrs.insert(baseAddr).second, ErrorCode::kInvalid,
                           "duplicate buffer address in batch unregister");
        RegisteredRegion region;
        TE_CHECK_OR_RETURN(registeredMemory->FindRegionByBaseAddr(baseAddr, &region), ErrorCode::kNotFound,
                           "region is not registered");
        const auto entry = FindBackingEntry(backingEntries, region.backingBaseAddr, region.backingLength);
        TE_CHECK_OR_RETURN(entry != backingEntries->end() && entry->logicalRefCount > 0, ErrorCode::kRuntimeError,
                           "backing registry is inconsistent");
        baseAddrs->push_back(baseAddr);
        regions->push_back(region);
    }
    return Result::OK();
}

Result CollectBackingsToRemove(const std::vector<RegisteredRegion> &regions, const BackingEntries &backingEntries,
                               BackingEntries *backingsToRemove)
{
    for (const auto &entry : backingEntries) {
        size_t removedRefCount = 0;
        for (const auto &region : regions) {
            if (IsSameBacking(entry, region.backingBaseAddr, region.backingLength)) {
                ++removedRefCount;
            }
        }
        TE_CHECK_OR_RETURN(removedRefCount <= entry.logicalRefCount, ErrorCode::kRuntimeError,
                           "backing reference count underflow");
        if (removedRefCount > 0 && removedRefCount == entry.logicalRefCount) {
            backingsToRemove->push_back(entry);
        }
    }
    return Result::OK();
}

void ReleaseBackingRefCounts(BackingEntries *backingEntries, const std::vector<RegisteredRegion> &regions)
{
    for (const auto &region : regions) {
        auto entry = FindBackingEntry(backingEntries, region.backingBaseAddr, region.backingLength);
        if (entry != backingEntries->end()) {
            --entry->logicalRefCount;
        }
    }
    backingEntries->erase(
        std::remove_if(backingEntries->begin(), backingEntries->end(),
                       [](const TransferEngineState::BackingEntry &entry) { return entry.logicalRefCount == 0; }),
        backingEntries->end());
}

}  // namespace

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
    Result finalizeRc;
    do {
        finalizeRc = Finalize();
    } while (finalizeRc.GetCode() == ErrorCode::kNotReady);
}

Result TransferEngine::Initialize(const std::string &localHostname, const std::string &protocol,
                                  const std::string &deviceName)
{
    internal::InitializeLogging();
    std::string localHost;
    uint16_t localPort = 0;
    int32_t deviceId = -1;
    TE_RETURN_IF_ERROR(ParseTargetHostname(localHostname, &localHost, &localPort, true));
    TE_RETURN_IF_ERROR(ParseDeviceId(deviceName, &deviceId));

    std::lock_guard<std::mutex> lock(apiMutex_);
    TE_CHECK_OR_RETURN(!initialized_, ErrorCode::kInvalid, "transfer engine already initialized");

    registeredMemory_->OpenReadLeaseAdmission();
    connMgr_->Clear();

    localHost_ = localHost;
    localPort_ = localPort;
    deviceId_ = deviceId;
    rpcThreads_ = kDefaultRpcThreads;
    TE_RETURN_IF_ERROR(BindControlPortLocked());
    Result backendRc = InitializeAscendBackendLocked(protocol);
    if (backendRc.IsError()) {
        controlServer_->Stop();
        if (backend_ != nullptr) {
            backend_->FinalizeLocal();
        }
        return backendRc;
    }
    Result startRc = StartControlServerLocked();
    if (startRc.IsError()) {
        return startRc;
    }

    initialized_ = true;
    TE_LOG_INFO << "transfer engine initialize success"
                << ", local_host=" << localHost_ << ", local_port=" << localPort_ << ", device_id=" << deviceId_;
    return Result::OK();
}

Result TransferEngine::Initialize(const std::string &localHostname, const std::string &metadataServer,
                                  const std::string &protocol, const std::string &deviceName)
{
    const std::string metadataServerLower = ToLowerAscii(metadataServer);
    TE_CHECK_OR_RETURN(metadataServerLower.empty() || metadataServerLower == "p2phandshake", ErrorCode::kNotSupported,
                       "metadata_server should be empty or P2PHANDSHAKE for YuanRong TransferEngine");
    return Initialize(localHostname, protocol, deviceName);
}

Result TransferEngine::BindControlPortLocked()
{
    if (localPort_ != 0) {
        return controlServer_->Bind(localHost_, localPort_, &localPort_);
    }
    const RpcPortRange range = ReadRpcPortRangeFromEnv();
    if (!range.enabled) {
        return controlServer_->Bind(localHost_, 0, &localPort_);
    }
    TE_LOG_INFO << "transfer engine RPC port probing range"
                << ", min_port=" << range.minPort << ", max_port=" << range.maxPort;
    std::random_device randGen;
    std::uniform_int_distribution<int> portDist(range.minPort, range.maxPort);
    for (int attempt = 0; attempt < K_RPC_PORT_RANGE_BIND_ATTEMPTS; ++attempt) {
        const auto candidate = static_cast<uint16_t>(portDist(randGen));
        // Bind holds the listening socket, so a successful bind is race-free; occupied candidates
        // only log at vlog level during probing.
        Result rc = controlServer_->Bind(localHost_, candidate, &localPort_, ListenSocketFailureLogLevel::kVlog1);
        if (rc.IsOk()) {
            return rc;
        }
    }
    return TE_MAKE_STATUS(ErrorCode::kRuntimeError,
                          "no available RPC port within YR_TE_RPC_PORT_MIN/YR_TE_RPC_PORT_MAX range");
}

Result TransferEngine::InitializeAscendBackendLocked(const std::string &protocol)
{
    TE_RETURN_IF_ERROR(ValidateProtocol(protocol));
    if (!backendInjected_) {
        std::shared_ptr<IDataPlaneBackend> selectedBackend;
        TE_RETURN_IF_ERROR(CreateAscendBackend(selectedBackend));
        backend_ = std::move(selectedBackend);
    }
    TE_CHECK_OR_RETURN(backend_ != nullptr, ErrorCode::kInvalid, "backend is null");
    if (backendInjected_) {
        TE_CHECK_OR_RETURN(backend_->BackendKind() == "ascend", ErrorCode::kNotSupported,
                           "injected backend kind must be ascend");
    }

    TE_LOG_INFO << "transfer engine initialize start"
                << ", protocol=" << protocol << ", local_host=" << localHost_ << ", local_port=" << localPort_
                << ", device_id=" << deviceId_ << ", backend=" << backend_->BackendKind()
                << ", backend_injected=" << backendInjected_ << ", rpc_threads=" << kDefaultRpcThreads;

    if (backend_->RequiresAclRuntime()) {
        TE_RETURN_IF_ERROR(internal::EnsureAclSetDeviceForCurrentThread(deviceId_));
    }
    Result backendInitRc = backend_->InitializeLocal(localHost_, localPort_, deviceId_);
    if (backendInitRc.IsError()) {
        TE_LOG_ERROR << "backend initialize failed"
                     << ", backend=" << backend_->BackendKind() << ", local_host=" << localHost_
                     << ", local_port=" << localPort_ << ", device_id=" << deviceId_
                     << ", reason=" << backendInitRc.ToString();
        return backendInitRc;
    }
    return Result::OK();
}

Result TransferEngine::StartControlServerLocked()
{
    controlService_ =
        CreateTransferControlService(localHost_, localPort_, deviceId_, connMgr_, registeredMemory_, backend_);
    Result startRc = controlServer_->Start(localHost_, localPort_, controlService_, rpcThreads_);
    if (startRc.IsError()) {
        TE_LOG_ERROR << "control server start failed"
                     << ", local_host=" << localHost_ << ", local_port=" << localPort_ << ", device_id=" << deviceId_
                     << ", reason=" << startRc.ToString();
        controlService_.reset();
        controlServer_->Stop();
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

std::string TransferEngine::GetRoutePolicy()
{
    std::lock_guard<std::mutex> lock(apiMutex_);
    if (!initialized_ || backend_ == nullptr) {
        return "";
    }
    return backend_->RoutePolicy();
}

Result TransferEngine::RegisterMemory(uintptr_t bufferAddrRegisrterch, size_t length)
{
    return RegisterMemoryEx(MemoryRegistration{ bufferAddrRegisrterch, length, bufferAddrRegisrterch, length });
}

Result TransferEngine::BatchRegisterMemory(const std::vector<uintptr_t> &bufferAddrs,
                                           const std::vector<size_t> &lengths)
{
    TE_CHECK_OR_RETURN(!bufferAddrs.empty(), ErrorCode::kInvalid, "bufferAddrs is empty");
    TE_CHECK_OR_RETURN(bufferAddrs.size() == lengths.size(), ErrorCode::kInvalid, "bufferAddrs/lengths size mismatch");
    TE_CHECK_OR_RETURN(bufferAddrs.size() <= K_MAX_BATCH_REGISTRATION_ITEMS, ErrorCode::kInvalid,
                       "registration batch item count exceeds limit");

    std::vector<MemoryRegistration> registrations;
    registrations.reserve(bufferAddrs.size());
    for (size_t index = 0; index < bufferAddrs.size(); ++index) {
        registrations.push_back(
            MemoryRegistration{ bufferAddrs[index], lengths[index], bufferAddrs[index], lengths[index] });
    }
    return BatchRegisterMemoryEx(registrations);
}

Result TransferEngine::RegisterMemoryEx(const MemoryRegistration &registration)
{
    return BatchRegisterMemoryEx({ registration });
}

Result TransferEngine::BatchRegisterMemoryEx(const std::vector<MemoryRegistration> &registrations)
{
    TE_CHECK_OR_RETURN(!registrations.empty(), ErrorCode::kInvalid, "registrations is empty");
    TE_CHECK_OR_RETURN(registrations.size() <= K_MAX_BATCH_REGISTRATION_ITEMS, ErrorCode::kInvalid,
                       "registration batch item count exceeds limit");

    std::lock_guard<std::mutex> lock(apiMutex_);
    TE_CHECK_OR_RETURN(initialized_, ErrorCode::kNotReady, "transfer engine not initialized");
    TE_CHECK_OR_RETURN(!finalizing_, ErrorCode::kNotReady, "transfer engine is finalizing");
    TE_CHECK_OR_RETURN(!backendDegraded_, ErrorCode::kNotReady,
                       "transfer engine backend is degraded; finalize and reinitialize it");
    TE_CHECK_OR_RETURN(deviceId_ >= 0, ErrorCode::kNotReady, "device_id is invalid");

    std::vector<RegisteredRegion> logicalRegions;
    BackingEntries newBackings;
    TE_RETURN_IF_ERROR(
        PlanBatchRegistration(registrations, deviceId_, state_->backingEntries, &logicalRegions, &newBackings));
    TE_CHECK_OR_RETURN(registeredMemory_->CanAddRegions(logicalRegions), ErrorCode::kInvalid,
                       "logical memory range is invalid or overlaps an existing registration");

    BackingEntries registeredBackings;
    registeredBackings.reserve(newBackings.size());
    for (const auto &backing : newBackings) {
        Result backendRegRc = backend_->RegisterLocalMemory(backing.addr, backing.length);
        if (backendRegRc.IsError()) {
            if (!UnregisterAllBackings(backend_.get(), registeredBackings)) {
                MarkBackendDegraded();
                return TE_MAKE_STATUS(ErrorCode::kRuntimeError,
                                      "memory registration failed and rollback failed; backend is degraded");
            }
            return backendRegRc;
        }
        registeredBackings.push_back(backing);
    }

    if (!registeredMemory_->AddRegions(logicalRegions)) {
        if (!UnregisterAllBackings(backend_.get(), registeredBackings)) {
            MarkBackendDegraded();
            return TE_MAKE_STATUS(ErrorCode::kRuntimeError,
                                  "logical registration commit and backend rollback failed; backend is degraded");
        }
        return TE_MAKE_STATUS(ErrorCode::kInvalid, "failed to add logical registered regions");
    }

    CommitBackings(&state_->backingEntries, newBackings, logicalRegions);
    TE_LOG_INFO << "batch register memory success"
                << ", device_id=" << deviceId_ << ", logical_count=" << logicalRegions.size()
                << ", new_backing_count=" << newBackings.size();
    return Result::OK();
}

Result TransferEngine::UnregisterMemory(uintptr_t bufferAddrRegisrterch)
{
    return BatchUnregisterMemory({ bufferAddrRegisrterch });
}

Result TransferEngine::BatchUnregisterMemory(const std::vector<uintptr_t> &bufferAddrs)
{
    std::lock_guard<std::mutex> lock(apiMutex_);
    TE_CHECK_OR_RETURN(initialized_, ErrorCode::kNotReady, "transfer engine not initialized");
    TE_CHECK_OR_RETURN(!finalizing_, ErrorCode::kNotReady, "transfer engine is finalizing");
    TE_CHECK_OR_RETURN(!backendDegraded_, ErrorCode::kNotReady,
                       "transfer engine backend is degraded; finalize and reinitialize it");
    TE_CHECK_OR_RETURN(!bufferAddrs.empty(), ErrorCode::kInvalid, "bufferAddrs is empty");
    TE_CHECK_OR_RETURN(bufferAddrs.size() <= K_MAX_BATCH_REGISTRATION_ITEMS, ErrorCode::kInvalid,
                       "unregistration batch item count exceeds limit");

    std::vector<uint64_t> baseAddrs;
    std::vector<RegisteredRegion> regions;
    TE_RETURN_IF_ERROR(
        PlanBatchUnregister(bufferAddrs, registeredMemory_.get(), &state_->backingEntries, &baseAddrs, &regions));

    BackingEntries backingsToRemove;
    TE_RETURN_IF_ERROR(CollectBackingsToRemove(regions, state_->backingEntries, &backingsToRemove));

    std::vector<RegisteredRegion> removedRegions;
    const auto removeRc = registeredMemory_->RemoveByBaseAddrsIfNoActiveLease(baseAddrs, &removedRegions);
    if (removeRc == RegisteredMemoryTable::RemoveResult::K_BUSY) {
        return Result(ErrorCode::kNotReady, "one or more regions have an active read lease");
    }
    TE_CHECK_OR_RETURN(removeRc == RegisteredMemoryTable::RemoveResult::K_REMOVED, ErrorCode::kNotFound,
                       "one or more regions are not registered");

    BackingEntries removedBackings;
    for (const auto &backing : backingsToRemove) {
        Result backendRc = backend_->UnregisterLocalMemory(backing.addr, backing.length);
        if (backendRc.IsError()) {
            bool rollbackOk = ReregisterAllBackings(backend_.get(), removedBackings);
            rollbackOk = registeredMemory_->AddRegions(removedRegions) && rollbackOk;
            if (!rollbackOk) {
                MarkBackendDegraded();
                return TE_MAKE_STATUS(ErrorCode::kRuntimeError,
                                      "memory unregistration failed and rollback failed; backend is degraded");
            }
            return backendRc;
        }
        removedBackings.push_back(backing);
    }

    ReleaseBackingRefCounts(&state_->backingEntries, regions);
    TE_LOG_INFO << "batch unregister memory success, count=" << bufferAddrs.size();
    return Result::OK();
}

void TransferEngine::MarkBackendDegraded()
{
    backend_->FinalizeLocal();
    backendDegraded_ = true;
    registeredMemory_->Clear();
    state_->backingEntries.clear();
}

Result TransferEngine::TransferSyncRead(const std::string &targetHostname, uintptr_t buffer,
                                        uintptr_t peerBufferAddress, size_t length)
{
    return BatchTransferSyncRead(targetHostname, { buffer }, { peerBufferAddress }, { length });
}

Result TransferEngine::BatchTransferSyncRead(const std::string &targetHostname, const std::vector<uintptr_t> &buffers,
                                             const std::vector<uintptr_t> &peerBufferAddresses,
                                             const std::vector<size_t> &lengths)
{
    internal::DumpProcessEnvironment("batch_transfer_sync_read_begin");
    TE_CHECK_OR_RETURN(!buffers.empty(), ErrorCode::kInvalid, "buffers is empty");
    TE_CHECK_OR_RETURN(buffers.size() <= K_MAX_BATCH_READ_ITEMS, ErrorCode::kInvalid,
                       "batch read item count exceeds limit");
    TE_CHECK_OR_RETURN(buffers.size() == peerBufferAddresses.size() && buffers.size() == lengths.size(),
                       ErrorCode::kInvalid, "buffers/peerBufferAddresses/lengths size mismatch");

    SyncReadContext ctx;
    ctx.targetHostname = targetHostname;
    ctx.buffers = &buffers;
    ctx.peerBufferAddresses = &peerBufferAddresses;
    ctx.lengths = &lengths;
    TE_RETURN_IF_ERROR(ParseTargetHostname(targetHostname, &ctx.peerHost, &ctx.peerPort));
    for (size_t i = 0; i < buffers.size(); ++i) {
        TE_CHECK_OR_RETURN(buffers[i] > 0, ErrorCode::kInvalid, "buffer should be positive");
        TE_CHECK_OR_RETURN(peerBufferAddresses[i] > 0, ErrorCode::kInvalid, "peerBufferAddress should be positive");
        TE_CHECK_OR_RETURN(lengths[i] > 0, ErrorCode::kInvalid, "length should be positive");
    }

    TE_RETURN_IF_ERROR(EnterSyncRead(&ctx));
    ScopeExit finishGuard([this]() { LeaveSyncRead(); });

    TE_VLOG_1 << "batch sync read begin"
              << ", item_count=" << buffers.size()
              << ", target_hostname=" << targetHostname
              << ", peer=" << ctx.peerHost << ":" << ctx.peerPort
              << ", request_id_start=" << ctx.requestIdStart
              << ", backend=" << backend_->BackendKind();

    if (backend_->SupportsReceiverDrivenRead()) {
        return BatchTransferSyncReadReceiverDriven(ctx);
    }
    return BatchTransferSyncReadLegacy(ctx);
}

Result TransferEngine::EnterSyncRead(SyncReadContext *ctx)
{
    std::lock_guard<std::mutex> lock(apiMutex_);
    TE_CHECK_OR_RETURN(initialized_, ErrorCode::kNotReady, "transfer engine not initialized");
    TE_CHECK_OR_RETURN(!finalizing_, ErrorCode::kNotReady, "transfer engine is finalizing");
    TE_CHECK_OR_RETURN(!backendDegraded_, ErrorCode::kNotReady,
                       "transfer engine backend is degraded; finalize and reinitialize it");
    ctx->localHost = localHost_;
    ctx->localPort = localPort_;
    ctx->deviceId = deviceId_;
    ctx->requestIdStart = nextRequestId_;
    nextRequestId_ += static_cast<uint64_t>(ctx->buffers->size());
    ++inFlightSyncReads_;
    return Result::OK();
}

void TransferEngine::LeaveSyncRead()
{
    std::lock_guard<std::mutex> lock(apiMutex_);
    if (inFlightSyncReads_ > 0) {
        --inFlightSyncReads_;
    }
    if (finalizing_ && inFlightSyncReads_ == 0) {
        apiCv_.notify_all();
    }
}

ConnectionSpec TransferEngine::BuildReadConnectionSpec(const SyncReadContext &ctx, int32_t ownerDeviceId) const
{
    ConnectionSpec spec;
    spec.localHost = ctx.localHost;
    spec.localPort = ctx.localPort;
    spec.localDeviceId = ctx.deviceId;
    spec.peerHost = ctx.peerHost;
    spec.peerPort = ctx.peerPort;
    spec.peerDeviceId = ownerDeviceId;
    return spec;
}

void TransferEngine::InvalidateReadRoute(const ConnectionSpec &spec, int32_t ownerDeviceId, bool evictEndpointCache)
{
    backend_->AbortConnection(spec);
    ConnectionKey key{ spec.localDeviceId, spec.peerHost, spec.peerPort, ownerDeviceId };
    connMgr_->MarkStale(key);
    if (evictEndpointCache) {
        std::lock_guard<std::mutex> lock(endpointCacheMutex_);
        state_->endpointOwnerDeviceCache.erase(spec.peerHost + ":" + std::to_string(spec.peerPort));
    }
}

void TransferEngine::ReleaseReadLeaseQuietly(const SyncReadContext &ctx, const BatchReadTriggerResponse &rsp)
{
    if (rsp.readLeaseId == 0) {
        return;
    }
    ReleaseReadLeaseRequest releaseReq;
    releaseReq.readLeaseId = rsp.readLeaseId;
    releaseReq.requesterHost = ctx.localHost;
    releaseReq.requesterPort = ctx.localPort;
    releaseReq.requesterDeviceId = ctx.deviceId;
    ReleaseReadLeaseResponse releaseRsp;
    Result releaseRc = controlClient_->ReleaseReadLease(ctx.peerHost, ctx.peerPort, releaseReq, &releaseRsp);
    if (releaseRc.IsOk()) {
        releaseRc = RpcCodeToStatus(releaseRsp.code, releaseRsp.msg);
    }
    if (releaseRc.IsError()) {
        TE_LOG_WARNING << "release hixl read lease rpc failed"
                       << ", read_lease_id=" << rsp.readLeaseId << ", reason=" << releaseRc.ToString();
    }
}

Result TransferEngine::TriggerBatchReadRpc(const SyncReadContext &ctx, const ConnectionSpec &spec,
    int32_t ownerDeviceId, BatchReadTriggerResponse *rsp)
{
    BatchReadTriggerRequest req;
    req.requesterHost = ctx.localHost;
    req.requesterPort = ctx.localPort;
    req.requesterDeviceId = ctx.deviceId;
    req.ownerDeviceId = ownerDeviceId;
    req.items.reserve(ctx.buffers->size());
    for (size_t i = 0; i < ctx.buffers->size(); ++i) {
        BatchReadItem one;
        one.requestId = ctx.requestIdStart + static_cast<uint64_t>(i);
        one.remoteAddr = static_cast<uint64_t>((*ctx.peerBufferAddresses)[i]);
        one.length = static_cast<uint64_t>((*ctx.lengths)[i]);
        req.items.push_back(one);
    }

    Result triggerRc = controlClient_->BatchReadTrigger(ctx.peerHost, ctx.peerPort, req, rsp);
    if (triggerRc.IsError()) {
        TE_LOG_ERROR << "batch sync read trigger rpc failed, reason=" << triggerRc.ToString();
        InvalidateReadRoute(spec, ownerDeviceId, false);
        return triggerRc;
    }
    Result rpcStatus = RpcCodeToStatus(rsp->code, rsp->msg);
    if (rpcStatus.IsError()) {
        TE_LOG_ERROR << "batch sync read trigger rejected, failed_item_index=" << rsp->failedItemIndex
                     << ", reason=" << rsp->msg;
        InvalidateReadRoute(spec, ownerDeviceId, false);
        return rpcStatus;
    }
    return Result::OK();
}

std::vector<TransferReadOp> TransferEngine::BuildReadOps(const SyncReadContext &ctx) const
{
    std::vector<TransferReadOp> ops;
    ops.reserve(ctx.buffers->size());
    for (size_t i = 0; i < ctx.buffers->size(); ++i) {
        ops.push_back(TransferReadOp{ static_cast<uint64_t>((*ctx.buffers)[i]),
                                      static_cast<uint64_t>((*ctx.peerBufferAddresses)[i]),
                                      static_cast<uint64_t>((*ctx.lengths)[i]) });
    }
    return ops;
}

TransferEngine::ReceiverReadOutcome TransferEngine::AttemptReceiverDrivenRead(const SyncReadContext &ctx,
                                                                              int32_t attempt, Result *failRc)
{
    int32_t ownerDeviceId = -1;
    uint64_t ownerMemGeneration = 0;
    Result connRc = BuildConnectionIfNeeded(ctx.peerHost, ctx.peerPort, &ownerDeviceId, &ownerMemGeneration);
    if (connRc.IsError()) {
        TE_LOG_ERROR << "batch sync read build hixl connection failed, reason=" << connRc.ToString();
        *failRc = connRc;
        return ReceiverReadOutcome::K_FAIL;
    }
    const ConnectionSpec spec = BuildReadConnectionSpec(ctx, ownerDeviceId);

    BatchReadTriggerResponse rsp;
    Result triggerRc = TriggerBatchReadRpc(ctx, spec, ownerDeviceId, &rsp);
    if (triggerRc.IsError()) {
        *failRc = triggerRc;
        return ReceiverReadOutcome::K_FAIL;
    }
    if (rsp.ownerMemGeneration != ownerMemGeneration) {
        ReleaseReadLeaseQuietly(ctx, rsp);
        InvalidateReadRoute(spec, ownerDeviceId, true);
        TE_LOG_WARNING << "hixl owner memory generation changed during read authorization"
                       << ", cached_generation=" << ownerMemGeneration
                       << ", trigger_generation=" << rsp.ownerMemGeneration << ", attempt=" << attempt;
        return ReceiverReadOutcome::K_RETRY;
    }

    // Use the backend-configured HIXL transfer timeout.
    Result readRc = backend_->TransferSyncRead(spec, BuildReadOps(ctx), 0);
    ReleaseReadLeaseQuietly(ctx, rsp);
    if (readRc.IsError()) {
        InvalidateReadRoute(spec, ownerDeviceId, true);
        if (IsRetryableHixlReadFailure(readRc) && (attempt + 1) < K_MAX_HIXL_READ_ATTEMPTS) {
            TE_LOG_WARNING << "retry hixl sync read after route cleanup"
                           << ", target_hostname=" << ctx.targetHostname
                           << ", attempt=" << (attempt + 1)
                           << ", reason=" << readRc.ToString();
            return ReceiverReadOutcome::K_RETRY;
        }
        TE_LOG_ERROR << "hixl transfer sync read failed, reason=" << readRc.ToString();
        *failRc = readRc;
        return ReceiverReadOutcome::K_FAIL;
    }
    TE_VLOG_1 << "hixl batch sync read success"
              << ", item_count=" << ctx.buffers->size() << ", target_hostname=" << ctx.targetHostname
              << ", owner_mem_generation=" << ownerMemGeneration;
    return ReceiverReadOutcome::K_DONE;
}

Result TransferEngine::BatchTransferSyncReadReceiverDriven(const SyncReadContext &ctx)
{
    std::vector<TransferMemoryRegion> destRegions;
    destRegions.reserve(ctx.buffers->size());
    for (size_t i = 0; i < ctx.buffers->size(); ++i) {
        destRegions.push_back(
            TransferMemoryRegion{ static_cast<uint64_t>((*ctx.buffers)[i]), static_cast<uint64_t>((*ctx.lengths)[i]) });
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

    for (int32_t attempt = 0; attempt < K_MAX_HIXL_READ_ATTEMPTS; ++attempt) {
        Result failRc;
        const auto outcome = AttemptReceiverDrivenRead(ctx, attempt, &failRc);
        if (outcome == ReceiverReadOutcome::K_DONE) {
            return Result::OK();
        }
        if (outcome == ReceiverReadOutcome::K_RETRY) {
            continue;
        }
        return failRc;
    }
    return TE_MAKE_STATUS(ErrorCode::kNotReady, "owner memory generation keeps changing during hixl read");
}

Result TransferEngine::BatchTransferSyncReadLegacy(const SyncReadContext &ctx)
{
    int32_t ownerDeviceId = -1;
    uint64_t ownerMemGeneration = 0;
    Result connRc = BuildConnectionIfNeeded(ctx.peerHost, ctx.peerPort, &ownerDeviceId, &ownerMemGeneration);
    if (connRc.IsError()) {
        TE_LOG_ERROR << "batch sync read build connection failed, reason=" << connRc.ToString();
        return connRc;
    }
    const ConnectionSpec spec = BuildReadConnectionSpec(ctx, ownerDeviceId);

    for (size_t i = 0; i < ctx.buffers->size(); ++i) {
        Result postRecvRc = backend_->PostRecv(spec, static_cast<uint64_t>((*ctx.buffers)[i]),
                                               static_cast<uint64_t>((*ctx.lengths)[i]));
        if (postRecvRc.IsError()) {
            TE_LOG_ERROR << "batch sync read post recv failed, item_index=" << i
                         << ", reason=" << postRecvRc.ToString();
            InvalidateReadRoute(spec, ownerDeviceId, false);
            return postRecvRc;
        }
    }

    BatchReadTriggerResponse rsp;
    Result triggerRc = TriggerBatchReadRpc(ctx, spec, ownerDeviceId, &rsp);
    if (triggerRc.IsError()) {
        return triggerRc;
    }

    for (size_t i = 0; i < ctx.buffers->size(); ++i) {
        Result waitRc = backend_->WaitRecv(spec, kDefaultRecvWaitTimeoutMs);
        if (waitRc.IsError()) {
            TE_LOG_ERROR << "batch sync read wait recv failed, item_index=" << i << ", reason=" << waitRc.ToString();
            ConnectionKey key{ ctx.deviceId, ctx.peerHost, ctx.peerPort, ownerDeviceId };
            connMgr_->MarkStale(key);
            return waitRc;
        }
    }
    TE_VLOG_1 << "batch sync read success, item_count=" << ctx.buffers->size()
              << ", target_hostname=" << ctx.targetHostname;
    return Result::OK();
}

Result TransferEngine::Finalize()
{
    std::lock_guard<std::mutex> finalizeLock(finalizeMutex_);
    bool startShutdown = false;
    {
        std::unique_lock<std::mutex> lock(apiMutex_);
        if (!initialized_) {
            return Result::OK();
        }
        if (!finalizing_) {
            TE_LOG_INFO << "transfer engine finalize start"
                        << ", local_host=" << localHost_ << ", local_port=" << localPort_ << ", device_id=" << deviceId_
                        << ", inflight_sync_reads=" << inFlightSyncReads_;
            finalizing_ = true;
            startShutdown = true;
            apiCv_.wait(lock, [this]() { return inFlightSyncReads_ == 0; });
        }
    }

    if (startShutdown && controlService_ != nullptr) {
        controlService_->BeginShutdown();
    }
    if (!registeredMemory_->WaitForNoActiveReadLeases(K_FINALIZE_LEASE_WAIT_TIMEOUT_MS)) {
        TE_LOG_WARNING << "transfer engine finalize deferred by active remote read leases";
        return Result(ErrorCode::kNotReady, "active remote read leases did not drain before finalize deadline");
    }

    TeardownEngineState();
    TE_LOG_INFO << "transfer engine finalize success";
    internal::FlushLogs();
    return Result::OK();
}

void TransferEngine::TeardownEngineState()
{
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
        registeredMemory_->Clear();
        connMgr_->Clear();
        state_->backingEntries.clear();
        initialized_ = false;
        finalizing_ = false;
        backendDegraded_ = false;
        localHost_.clear();
        localPort_ = 0;
        deviceId_ = -1;
        rpcThreads_ = 0;
        nextRequestId_ = 1;
    }
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

    if (TryReuseCachedConnection(peerHost, peerPort, cached.ownerDeviceId, cached.ownerMemGeneration, ownerDeviceId,
                                 ownerMemGeneration)) {
        return Result::OK();
    }
    return BuildConnectionOnce(peerHost, peerPort, ownerDeviceId, ownerMemGeneration);
}

bool TransferEngine::TryReuseCachedConnection(const std::string &peerHost, uint16_t peerPort,
                                              int32_t cachedOwnerDeviceId, uint64_t cachedOwnerMemGeneration,
                                              int32_t *ownerDeviceId, uint64_t *ownerMemGeneration)
{
    ConnectionKey key{ deviceId_, peerHost, peerPort, cachedOwnerDeviceId };
    if (!connMgr_->HasReadyConnection(key)) {
        return false;
    }
    TE_VLOG_1 << "reuse cached connection"
              << ", peer=" << peerHost << ":" << peerPort << ", owner_device_id=" << cachedOwnerDeviceId
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
            return true;
        }
    }
    TE_LOG_WARNING << "cached connection stale, rebuilding"
                   << ", peer=" << peerHost << ":" << peerPort << ", owner_device_id=" << cachedOwnerDeviceId
                   << ", cached_owner_mem_generation=" << cachedOwnerMemGeneration;
    connMgr_->MarkStale(key);
    std::lock_guard<std::mutex> lock(endpointCacheMutex_);
    state_->endpointOwnerDeviceCache.erase(peerHost + ":" + std::to_string(peerPort));
    return false;
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
                                                     const std::string &rootInfo, ExchangeRootInfoResponse *exchangeRsp)
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
                     << ", peer=" << peerHost << ":" << peerPort << ", owner_device_id=" << exchangeRsp.ownerDeviceId
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
                   << ", peer=" << peerHost << ":" << peerPort << ", owner_device_id=" << ownerDeviceId
                   << ", retry_count=" << kConnReadyRetryCount;
    return TE_MAKE_STATUS(ErrorCode::kNotReady,
                          "connection is not ready, peer=" + peerHost + ":" + std::to_string(peerPort) + ", device_id="
                              + std::to_string(deviceId_) + ", owner_device_id=" + std::to_string(ownerDeviceId));
}

std::string TransferEngine::CreateRootInfo() const
{
    return "te_root_info_v0_1_0";
}

}  // namespace datasystem
