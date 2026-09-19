/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include "internal/backend/ascend/ascend_backend.h"

#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <map>
#include <utility>
#include <vector>

#include <hixl/hixl.h>

#include "internal/backend/ascend/hixl_config.h"
#include "internal/control_plane/socket_rpc_transport.h"
#include "internal/log/environment_dump.h"
#include "internal/log/logging.h"
#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr uint64_t K_REGISTER_BASE_ALIGNMENT = 2ULL * 1024ULL * 1024ULL;

// Mooncake ADXL shares the 20000 + 100 * device_id segment convention; 22000 keeps 20 device
// segments clear so co-located Mooncake and YuanRong engines never probe the same ports.
constexpr int32_t K_DEFAULT_HIXL_BASE_PORT = 22000;
constexpr int32_t K_PORT_SEGMENT_SIZE = 100;
constexpr int32_t K_DEFAULT_CONNECT_TIMEOUT_MS = 10000;
constexpr int32_t K_DEFAULT_TRANSFER_TIMEOUT_MS = 10000;
constexpr int32_t K_DEFAULT_READ_LEASE_TTL_MS = 30000;
constexpr int32_t K_READ_LEASE_TIMEOUT_MARGIN_MS = 1000;
constexpr int32_t K_MAX_TCP_PORT = 65535;
constexpr int32_t K_DECIMAL_BASE = 10;
constexpr size_t K_MAX_TRANSFER_OPS_PER_CALL = 4096;
constexpr char K_OPTION_AUTO_CONNECT[] = "AutoConnect";
constexpr char K_OPTION_GLOBAL_RESOURCE_CONFIG[] = "GlobalResourceConfig";
constexpr char K_OPTION_LOCAL_COMM_RES[] = "LocalCommRes";

struct HixlInitConfig {
    std::map<hixl::AscendString, hixl::AscendString> options;
    std::string localHost;
    std::string endpoint;
    std::string routePolicy;
    std::string requestedCsMode;
    std::string bufferPool;
    std::string localCommRes;
    std::string requestedAutoConnect;
    std::string autoConnectOption;
    std::string globalResourceConfig;
    std::string rdmaTrafficClass;
    std::string rdmaServiceLevel;
    uint16_t localPort = 0;
    int32_t localDeviceId = -1;
    int32_t connectTimeoutMs = 0;
    int32_t transferTimeoutMs = 0;
    bool autoConnectEnabled = false;
    bool localCommResConfigured = false;
    bool globalResourceConfigUserConfigured = false;
    bool globalResourceConfigConfigured = false;
    bool rdmaTrafficClassConfigured = false;
    bool rdmaServiceLevelConfigured = false;
    HixlEngineMode engineMode = HixlEngineMode::kLegacy;
};

std::string GetEnvOrDefault(const char *name, const std::string &defaultValue)
{
    const char *value = std::getenv(name);
    return value == nullptr || value[0] == '\0' ? defaultValue : std::string(value);
}

bool GetEnvIfSet(const char *name, std::string &value)
{
    const char *env = std::getenv(name);
    if (env == nullptr || env[0] == '\0') {
        return false;
    }
    value = env;
    return true;
}

Result HixlStatusToResult(hixl::Status status, const std::string &where)
{
    if (status == hixl::SUCCESS) {
        return Result::OK();
    }
    ErrorCode code = ErrorCode::kRuntimeError;
    if (status == hixl::PARAM_INVALID) {
        code = ErrorCode::kInvalid;
    } else if (status == hixl::TIMEOUT || status == hixl::NOT_CONNECTED || status == hixl::RESOURCE_EXHAUSTED) {
        code = ErrorCode::kNotReady;
    } else if (status == hixl::UNSUPPORTED) {
        code = ErrorCode::kNotSupported;
    }
    return TE_MAKE_STATUS(code, where + " failed, hixl status=" + std::to_string(status));
}

void LogDisconnectFailure(hixl::Status status, const std::string &where, const std::string &endpoint, int32_t timeoutMs)
{
    if (status == hixl::SUCCESS || status == hixl::NOT_CONNECTED) {
        return;
    }
    TE_LOG_WARNING << where << " failed"
                   << ", remote_hixl_endpoint=" << endpoint << ", timeout_ms=" << timeoutMs
                   << ", hixl_status=" << status;
}

bool IsSupportedRoute(const std::string &route)
{
    return route == "auto" || route == "hccs" || route == "roce";
}

bool IsRangeInside(uint64_t addr, uint64_t length, uint64_t regionAddr, uint64_t regionLength)
{
    if (length == 0 || regionLength == 0 || addr > std::numeric_limits<uint64_t>::max() - length ||
        regionAddr > std::numeric_limits<uint64_t>::max() - regionLength) {
        return false;
    }
    return addr >= regionAddr && addr + length <= regionAddr + regionLength;
}

bool ParseVisibleDevices(const std::string &value, std::vector<int32_t> *devices)
{
    devices->clear();
    std::string token;
    auto flushToken = [&token, devices]() -> bool {
        if (token.empty()) {
            return true;
        }
        int64_t parsed = 0;
        for (char ch : token) {
            if (ch < '0' || ch > '9') {
                return false;
            }
            parsed = parsed * 10 + static_cast<int64_t>(ch - '0');
            if (parsed > std::numeric_limits<int32_t>::max()) {
                return false;
            }
        }
        devices->push_back(static_cast<int32_t>(parsed));
        token.clear();
        return true;
    };

    for (char ch : value) {
        if (ch == ',') {
            if (!flushToken()) {
                return false;
            }
            continue;
        }
        if (std::isspace(static_cast<unsigned char>(ch))) {
            continue;
        }
        token.push_back(ch);
    }
    return flushToken();
}

int32_t ResolvePhysicalDeviceId(int32_t logicalDeviceId)
{
    std::string visibleDevices;
    if (!GetEnvIfSet("ASCEND_RT_VISIBLE_DEVICES", visibleDevices) &&
        !GetEnvIfSet("RT_ASCEND_VISIBLE_DEVICES", visibleDevices)) {
        return logicalDeviceId;
    }
    std::vector<int32_t> devices;
    if (!ParseVisibleDevices(visibleDevices, &devices) || logicalDeviceId < 0 ||
        static_cast<size_t>(logicalDeviceId) >= devices.size()) {
        return logicalDeviceId;
    }
    return devices[static_cast<size_t>(logicalDeviceId)];
}

#if defined(TRANSFER_ENGINE_HIXL_CS_AVAILABLE) || defined(TRANSFER_ENGINE_HIXL_AUTO_CONNECT_AVAILABLE)
bool QueryHixlCapability(hixl::FeatureType feature, const char *featureName)
{
    int32_t value = hixl::FEATURE_NOT_SUPPORTED;
    const hixl::Status status = hixl::Hixl::GetCapability(feature, value);
    if (status != hixl::SUCCESS) {
        TE_LOG_WARNING << "Hixl::GetCapability failed"
                       << ", feature=" << featureName
                       << ", hixl_status=" << status;
        return false;
    }
    return value == hixl::FEATURE_SUPPORTED;
}
#endif

bool QueryHixlCsCapability()
{
#ifdef TRANSFER_ENGINE_HIXL_CS_AVAILABLE
    return QueryHixlCapability(hixl::CLIENT_SERVER_COMM, "CLIENT_SERVER_COMM");
#else
    return false;
#endif
}

bool QueryHixlAutoConnectCapability()
{
#ifdef TRANSFER_ENGINE_HIXL_AUTO_CONNECT_AVAILABLE
    return QueryHixlCapability(hixl::AUTO_CONNECT, "AUTO_CONNECT");
#else
    return false;
#endif
}

Result BuildHixlInitConfig(const std::string &routePolicy, HixlInitConfig &config)
{
    config.bufferPool = GetEnvOrDefault("YR_TE_HIXL_BUFFER_POOL", "0:0");
    config.options[hixl::AscendString(hixl::OPTION_BUFFER_POOL)] = hixl::AscendString(config.bufferPool.c_str());
    config.requestedAutoConnect = GetEnvOrDefault("YR_TE_HIXL_AUTO_CONNECT", "auto");
    HixlAutoConnectConfig autoConnectConfig;
    TE_RETURN_IF_ERROR(ResolveHixlAutoConnectConfig(config.requestedAutoConnect, QueryHixlAutoConnectCapability(),
                                                    &autoConnectConfig));
    config.autoConnectEnabled = autoConnectConfig.enabled;
    config.autoConnectOption = std::move(autoConnectConfig.optionValue);
    config.options[hixl::AscendString(K_OPTION_AUTO_CONNECT)] =
        hixl::AscendString(config.autoConnectOption.c_str());
    config.globalResourceConfigUserConfigured =
        GetEnvIfSet("YR_TE_HIXL_GLOBAL_RESOURCE_CONFIG", config.globalResourceConfig);
    config.requestedCsMode = GetEnvOrDefault("YR_TE_HIXL_CS_MODE", K_DEFAULT_HIXL_CS_MODE);
    HixlCsConfigInput csInput;
    csInput.requestedMode = config.requestedCsMode;
    csInput.routePolicy = routePolicy;
    GetEnvIfSet("YR_TE_HIXL_LOCAL_COMM_RES", csInput.localCommRes);
    csInput.globalResourceConfig = config.globalResourceConfig;
    csInput.capabilityAvailable = QueryHixlCsCapability();
    csInput.legacyRoceEnabled = GetEnvOrDefault("HCCL_INTRA_ROCE_ENABLE", "0") == "1";
    HixlCsConfig csConfig;
    TE_RETURN_IF_ERROR(ResolveHixlCsConfig(csInput, &csConfig));
    config.engineMode = csConfig.engineMode;
    config.localCommRes = std::move(csConfig.localCommRes);
    config.localCommResConfigured = !config.localCommRes.empty();
    if (config.localCommResConfigured) {
        config.options[hixl::AscendString(K_OPTION_LOCAL_COMM_RES)] = hixl::AscendString(config.localCommRes.c_str());
    }
    config.globalResourceConfig = std::move(csConfig.globalResourceConfig);
    config.globalResourceConfigConfigured = !config.globalResourceConfig.empty();
    if (config.globalResourceConfigConfigured) {
        config.options[hixl::AscendString(K_OPTION_GLOBAL_RESOURCE_CONFIG)] =
            hixl::AscendString(config.globalResourceConfig.c_str());
    }
    config.rdmaTrafficClassConfigured =
        GetEnvIfSet("YR_TE_HIXL_RDMA_TC", config.rdmaTrafficClass) ||
        GetEnvIfSet("HCCL_RDMA_TC", config.rdmaTrafficClass);
    if (config.rdmaTrafficClassConfigured) {
        config.options[hixl::AscendString(hixl::OPTION_RDMA_TRAFFIC_CLASS)] =
            hixl::AscendString(config.rdmaTrafficClass.c_str());
    }
    config.rdmaServiceLevelConfigured =
        GetEnvIfSet("YR_TE_HIXL_RDMA_SL", config.rdmaServiceLevel) ||
        GetEnvIfSet("HCCL_RDMA_SL", config.rdmaServiceLevel);
    if (config.rdmaServiceLevelConfigured) {
        config.options[hixl::AscendString(hixl::OPTION_RDMA_SERVICE_LEVEL)] =
            hixl::AscendString(config.rdmaServiceLevel.c_str());
    }
    return Result::OK();
}

void LogHixlInitializeBegin(const HixlInitConfig &config)
{
    TE_LOG_INFO << "ascend backend initialize begin"
                << ", local_host=" << config.localHost << ", local_port=" << config.localPort
                << ", logical_device_id=" << config.localDeviceId
                << ", physical_device_id=" << ResolvePhysicalDeviceId(config.localDeviceId)
                << ", hixl_endpoint=" << config.endpoint << ", hixl_route_policy=" << config.routePolicy
                << ", hixl_engine_mode=" << HixlEngineModeName(config.engineMode)
                << ", requested_cs_mode=" << config.requestedCsMode
                << ", connect_timeout_ms=" << config.connectTimeoutMs
                << ", transfer_timeout_ms=" << config.transferTimeoutMs
                << ", buffer_pool=" << config.bufferPool
                << ", auto_connect_requested=" << config.requestedAutoConnect
                << ", auto_connect_enabled=" << config.autoConnectEnabled
                << ", local_comm_res_configured=" << config.localCommResConfigured
                << ", global_resource_config_configured=" << config.globalResourceConfigConfigured
                << ", global_resource_config_user_configured=" << config.globalResourceConfigUserConfigured
                << ", global_resource_config_bytes=" << config.globalResourceConfig.size()
                << ", rdma_traffic_class_configured=" << config.rdmaTrafficClassConfigured
                << ", rdma_traffic_class="
                << (config.rdmaTrafficClassConfigured ? config.rdmaTrafficClass : "(unset)")
                << ", rdma_service_level_configured=" << config.rdmaServiceLevelConfigured
                << ", rdma_service_level="
                << (config.rdmaServiceLevelConfigured ? config.rdmaServiceLevel : "(unset)");
}

uint64_t SaturatingBatchBytes(const std::vector<TransferReadOp> &ops, size_t base, size_t end)
{
    uint64_t batchBytes = 0;
    for (size_t i = base; i < end; ++i) {
        if (ops[i].length > std::numeric_limits<uint64_t>::max() - batchBytes) {
            return std::numeric_limits<uint64_t>::max();
        }
        batchBytes += ops[i].length;
    }
    return batchBytes;
}

}  // namespace

struct AscendBackend::Impl {
    hixl::Hixl engine;
    bool initialized = false;
};

AscendBackend::AscendBackend() : impl_(std::make_unique<Impl>())
{
    internal::InitializeLogging();
}

AscendBackend::~AscendBackend()
{
    FinalizeLocal();
}

void AscendBackend::FinalizeLocal()
{
    std::lock_guard<std::mutex> lock(mutex_);
    const Result disconnectRc = DisconnectAllLocked();
    if (disconnectRc.IsOk()) {
        for (auto &entry : registeredMems_) {
            if (entry.second.handle != nullptr) {
                const hixl::Status status = impl_->engine.DeregisterMem(entry.second.handle);
                if (status != hixl::SUCCESS) {
                    TE_LOG_WARNING << "Hixl::DeregisterMem during finalize failed"
                                   << ", addr=0x" << std::hex << entry.second.addr << std::dec
                                   << ", length=" << entry.second.length << ", local_device_id=" << localDeviceId_
                                   << ", hixl_status=" << status;
                }
            }
        }
    } else {
        TE_LOG_WARNING << "skip Hixl::DeregisterMem during finalize because disconnect-all failed"
                       << ", registered_region_count=" << registeredMems_.size()
                       << ", reason=" << disconnectRc.ToString();
    }
    registeredMems_.clear();
    if (impl_->initialized) {
        impl_->engine.Finalize();
        impl_->initialized = false;
    }
    connectedEndpoints_.clear();
    peerEndpointByConnection_.clear();
    engineMode_ = HixlEngineMode::kLegacy;
    autoConnectEnabled_ = false;
}

std::string AscendBackend::RoutePolicy() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return routePolicy_;
}

uint64_t AscendBackend::MemoryGeneration() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return memGeneration_;
}

Result AscendBackend::InitializeLocal(const std::string &localHost, uint16_t localPort, int32_t localDeviceId)
{
    TE_CHECK_OR_RETURN(localDeviceId >= 0, ErrorCode::kInvalid, "local_device_id is invalid");
    std::string routePolicy;
    TE_RETURN_IF_ERROR(ParseRoutePolicy(&routePolicy));

    std::string endpoint;
    TE_RETURN_IF_ERROR(BuildEndpoint(localHost, localDeviceId, &endpoint));
    HixlInitConfig config;
    TE_RETURN_IF_ERROR(BuildHixlInitConfig(routePolicy, config));

    connectTimeoutMs_ = GetEnvI32("YR_TE_HIXL_CONNECT_TIMEOUT_MS", K_DEFAULT_CONNECT_TIMEOUT_MS);
    transferTimeoutMs_ = GetEnvI32("YR_TE_HIXL_TRANSFER_TIMEOUT_MS", K_DEFAULT_TRANSFER_TIMEOUT_MS);
    const int32_t readLeaseTtlMs =
        GetEnvI32("YR_TE_HIXL_READ_LEASE_TTL_MS", K_DEFAULT_READ_LEASE_TTL_MS);
    TE_CHECK_OR_RETURN(transferTimeoutMs_ <= readLeaseTtlMs - K_READ_LEASE_TIMEOUT_MARGIN_MS, ErrorCode::kInvalid,
                       "hixl read lease ttl should exceed transfer timeout by at least 1000 ms");

    std::lock_guard<std::mutex> lock(mutex_);
    TE_CHECK_OR_RETURN(!impl_->initialized, ErrorCode::kInvalid, "ascend backend already initialized");
    localDeviceId_ = localDeviceId;
    routePolicy_ = routePolicy;
    engineMode_ = config.engineMode;
    autoConnectEnabled_ = config.autoConnectEnabled;
    hixlEndpoint_ = endpoint;

    internal::DumpProcessEnvironment("hixl_backend_initialize");
    config.localHost = localHost;
    config.localPort = localPort;
    config.localDeviceId = localDeviceId_;
    config.endpoint = hixlEndpoint_;
    config.routePolicy = routePolicy_;
    config.connectTimeoutMs = connectTimeoutMs_;
    config.transferTimeoutMs = transferTimeoutMs_;
    LogHixlInitializeBegin(config);
    const hixl::Status status = impl_->engine.Initialize(hixl::AscendString(hixlEndpoint_.c_str()), config.options);
    if (status != hixl::SUCCESS) {
        TE_LOG_ERROR << "ascend backend initialize failed"
                     << ", hixl_endpoint=" << hixlEndpoint_ << ", hixl_route_policy=" << routePolicy_
                     << ", hixl_engine_mode=" << HixlEngineModeName(engineMode_)
                     << ", local_device_id=" << localDeviceId_ << ", hixl_status=" << status;
        return HixlStatusToResult(status, "Hixl::Initialize");
    }
    impl_->initialized = true;

    TE_LOG_INFO << "ascend backend initialized"
                << ", local_host=" << localHost << ", local_port=" << localPort
                << ", local_device_id=" << localDeviceId_ << ", hixl_endpoint=" << hixlEndpoint_
                << ", hixl_route_policy=" << routePolicy_ << ", hixl_engine_mode=" << HixlEngineModeName(engineMode_)
                << ", connect_timeout_ms=" << connectTimeoutMs_ << ", transfer_timeout_ms=" << transferTimeoutMs_
                << ", read_lease_ttl_ms=" << readLeaseTtlMs;
    return Result::OK();
}

Result AscendBackend::RegisterLocalMemory(uint64_t addr, uint64_t length)
{
    TE_CHECK_OR_RETURN(addr > 0 && length > 0, ErrorCode::kInvalid, "invalid memory region");
    TE_CHECK_OR_RETURN(addr <= std::numeric_limits<uint64_t>::max() - length, ErrorCode::kInvalid,
                       "memory region overflow");
    std::lock_guard<std::mutex> lock(mutex_);
    bool registeredNew = false;
    TE_RETURN_IF_ERROR(RegisterOneLocked(addr, length, &registeredNew));
    if (registeredNew) {
        ++memGeneration_;
        (void)DisconnectAllLocked();
    }
    return Result::OK();
}

Result AscendBackend::UnregisterLocalMemory(uint64_t addr, uint64_t length)
{
    TE_CHECK_OR_RETURN(addr > 0 && length > 0, ErrorCode::kInvalid, "invalid memory region");
    std::lock_guard<std::mutex> lock(mutex_);
    bool unregistered = false;
    Result rc = UnregisterOneLocked(addr, length, true, &unregistered);
    if (unregistered) {
        ++memGeneration_;
    }
    return rc;
}

Result AscendBackend::PrepareReadDestinations(const std::vector<TransferMemoryRegion> &regions)
{
    std::lock_guard<std::mutex> lock(mutex_);
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "ascend backend is not initialized");
    for (size_t regionIndex = 0; regionIndex < regions.size(); ++regionIndex) {
        const auto &region = regions[regionIndex];
        if (region.addr == 0 || region.length == 0) {
            TE_LOG_ERROR << "invalid hixl read destination"
                         << ", region_index=" << regionIndex << ", addr=0x" << std::hex << region.addr << std::dec
                         << ", length=" << region.length;
            return TE_MAKE_STATUS(ErrorCode::kInvalid, "invalid hixl read destination");
        }
        bool registered = false;
        for (const auto &entry : registeredMems_) {
            if (IsRangeInside(region.addr, region.length, entry.second.addr, entry.second.length)) {
                registered = true;
                break;
            }
        }
        if (!registered) {
            TE_LOG_ERROR << "hixl read destination memory is not registered"
                         << ", region_index=" << regionIndex << ", addr=0x" << std::hex << region.addr << std::dec
                         << ", length=" << region.length << ", registered_region_count=" << registeredMems_.size()
                         << ", local_device_id=" << localDeviceId_;
            return TE_MAKE_STATUS(ErrorCode::kNotFound, "hixl read destination memory is not registered");
        }
    }
    return Result::OK();
}

Result AscendBackend::CreateRootInfo(std::string *rootInfoBytes)
{
    TE_CHECK_PTR_OR_RETURN(rootInfoBytes);
    std::lock_guard<std::mutex> lock(mutex_);
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "ascend backend is not initialized");
    HixlPeerInfo info;
    info.backendKind = BackendKind();
    info.endpoint = hixlEndpoint_;
    info.routePolicy = routePolicy_;
    info.engineMode = engineMode_;
    *rootInfoBytes = EncodeHixlPeerInfo(info);
    return Result::OK();
}

Result AscendBackend::InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes)
{
    HixlPeerInfo rootInfo;
    TE_RETURN_IF_ERROR(ParseHixlPeerInfo(rootInfoBytes, &rootInfo));
    TE_CHECK_OR_RETURN(rootInfo.backendKind == BackendKind(), ErrorCode::kNotSupported,
                       "peer root info backend is not ascend");
    TE_CHECK_OR_RETURN(rootInfo.routePolicy == routePolicy_, ErrorCode::kNotSupported,
                       "hixl route policy mismatch");
    TE_CHECK_OR_RETURN(rootInfo.engineMode == engineMode_, ErrorCode::kNotSupported,
                       "hixl engine mode mismatch");
    TE_CHECK_OR_RETURN(!rootInfo.endpoint.empty(), ErrorCode::kInvalid, "hixl peer endpoint is empty");

    std::lock_guard<std::mutex> lock(mutex_);
    TE_RETURN_IF_ERROR(ConnectLocked(ConnectionKey(spec), rootInfo.endpoint));
    return Result::OK();
}

Result AscendBackend::InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes)
{
    HixlPeerInfo rootInfo;
    Result parseRc = ParseHixlPeerInfo(rootInfoBytes, &rootInfo);
    if (parseRc.IsError()) {
        return parseRc;
    }
    TE_CHECK_OR_RETURN(rootInfo.backendKind == BackendKind(), ErrorCode::kNotSupported,
                       "peer root info backend is not ascend");
    TE_CHECK_OR_RETURN(rootInfo.routePolicy == routePolicy_, ErrorCode::kNotSupported,
                       "hixl route policy mismatch");
    TE_CHECK_OR_RETURN(rootInfo.engineMode == engineMode_, ErrorCode::kNotSupported,
                       "hixl engine mode mismatch");
    TE_LOG_INFO << "hixl owner accepted requester root info"
                << ", requester=" << spec.peerHost << ":" << spec.peerPort
                << ", requester_device_id=" << spec.peerDeviceId
                << ", requester_hixl_endpoint=" << rootInfo.endpoint
                << ", hixl_engine_mode=" << HixlEngineModeName(rootInfo.engineMode);
    return Result::OK();
}

Result AscendBackend::PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length)
{
    (void)spec;
    (void)localAddr;
    (void)length;
    return Result(ErrorCode::kNotSupported, "ascend backend uses receiver-driven read");
}

Result AscendBackend::PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length)
{
    (void)spec;
    (void)remoteAddr;
    (void)length;
    return Result(ErrorCode::kNotSupported, "ascend backend uses receiver-driven read");
}

Result AscendBackend::WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs)
{
    (void)spec;
    (void)timeoutMs;
    return Result(ErrorCode::kNotSupported, "ascend backend uses receiver-driven read");
}

Result AscendBackend::TransferSyncRead(const ConnectionSpec &spec, const std::vector<TransferReadOp> &ops,
                                       uint64_t timeoutMs)
{
    TE_CHECK_OR_RETURN(!ops.empty(), ErrorCode::kInvalid, "read ops is empty");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iter = peerEndpointByConnection_.find(ConnectionKey(spec));
    TE_CHECK_OR_RETURN(iter != peerEndpointByConnection_.end(), ErrorCode::kNotReady, "hixl connection not found");
    const std::string endpoint = iter->second;

    for (size_t base = 0; base < ops.size(); base += K_MAX_TRANSFER_OPS_PER_CALL) {
        const size_t end = std::min(base + K_MAX_TRANSFER_OPS_PER_CALL, ops.size());
        TE_RETURN_IF_ERROR(TransferReadBatchLocked(spec, ops, base, end, endpoint, timeoutMs));
    }
    TE_VLOG_1 << "hixl transfer sync read success"
                << ", peer=" << spec.peerHost << ":" << spec.peerPort
                << ", peer_device_id=" << spec.peerDeviceId
                << ", hixl_endpoint=" << endpoint
                << ", op_count=" << ops.size();
    return Result::OK();
}

Result AscendBackend::TransferReadBatchLocked(const ConnectionSpec &spec, const std::vector<TransferReadOp> &ops,
    size_t base, size_t end, const std::string &endpoint, uint64_t timeoutMs)
{
    std::vector<hixl::TransferOpDesc> descs;
    descs.reserve(end - base);
    for (size_t i = base; i < end; ++i) {
        TE_CHECK_OR_RETURN(ops[i].localAddr > 0 && ops[i].remoteAddr > 0 && ops[i].length > 0,
                           ErrorCode::kInvalid, "invalid hixl read op");
        hixl::TransferOpDesc desc{};
        desc.local_addr = static_cast<uintptr_t>(ops[i].localAddr);
        desc.remote_addr = static_cast<uintptr_t>(ops[i].remoteAddr);
        desc.len = static_cast<size_t>(ops[i].length);
        descs.push_back(desc);
    }
    const int32_t effectiveTimeout =
        (timeoutMs == 0 || timeoutMs > static_cast<uint64_t>(std::numeric_limits<int32_t>::max()))
            ? transferTimeoutMs_
            : static_cast<int32_t>(timeoutMs);
    TE_VLOG_1 << "hixl transfer sync read begin"
              << ", peer=" << spec.peerHost << ":" << spec.peerPort << ", peer_device_id=" << spec.peerDeviceId
              << ", remote_hixl_endpoint=" << endpoint << ", hixl_route_policy=" << routePolicy_
              << ", batch_begin=" << base << ", batch_op_count=" << descs.size()
              << ", total_op_count=" << ops.size() << ", transfer_timeout_ms=" << effectiveTimeout;
    const hixl::Status status =
        impl_->engine.TransferSync(hixl::AscendString(endpoint.c_str()), hixl::READ, descs, effectiveTimeout);
    Result rc = HixlStatusToResult(status, "Hixl::TransferSync(READ)");
    if (rc.IsOk()) {
        return rc;
    }
    TE_LOG_ERROR << "hixl transfer sync read failed"
                 << ", peer=" << spec.peerHost << ":" << spec.peerPort
                 << ", peer_device_id=" << spec.peerDeviceId << ", remote_hixl_endpoint=" << endpoint
                 << ", hixl_route_policy=" << routePolicy_ << ", batch_begin=" << base
                 << ", batch_op_count=" << descs.size() << ", total_op_count=" << ops.size()
                 << ", batch_bytes=" << SaturatingBatchBytes(ops, base, end)
                 << ", transfer_timeout_ms=" << effectiveTimeout << ", hixl_status=" << status;
    if (!autoConnectEnabled_) {
        const hixl::Status disconnectStatus =
            impl_->engine.Disconnect(hixl::AscendString(endpoint.c_str()), connectTimeoutMs_);
        LogDisconnectFailure(disconnectStatus, "Hixl::Disconnect after transfer failure", endpoint, connectTimeoutMs_);
        if (disconnectStatus == hixl::SUCCESS || disconnectStatus == hixl::NOT_CONNECTED) {
            connectedEndpoints_.erase(endpoint);
        }
    }
    peerEndpointByConnection_.erase(ConnectionKey(spec));
    return rc;
}

void AscendBackend::AbortConnection(const ConnectionSpec &spec)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = ConnectionKey(spec);
    auto iter = peerEndpointByConnection_.find(key);
    if (iter == peerEndpointByConnection_.end()) {
        return;
    }
    const std::string endpoint = iter->second;
    const hixl::Status status = impl_->engine.Disconnect(hixl::AscendString(endpoint.c_str()), connectTimeoutMs_);
    LogDisconnectFailure(status, "Hixl::Disconnect during abort", endpoint, connectTimeoutMs_);
    if (status == hixl::SUCCESS || status == hixl::NOT_CONNECTED) {
        connectedEndpoints_.erase(endpoint);
    }
    peerEndpointByConnection_.erase(iter);
}

std::string AscendBackend::ConnectionKey(const ConnectionSpec &spec)
{
    return spec.localHost + ":" + std::to_string(spec.localPort) + ":" + std::to_string(spec.localDeviceId) + "|" +
           spec.peerHost + ":" + std::to_string(spec.peerPort) + ":" + std::to_string(spec.peerDeviceId);
}

Result AscendBackend::ParseRoutePolicy(std::string *routePolicy)
{
    TE_CHECK_PTR_OR_RETURN(routePolicy);
    std::string route = GetEnvOrDefault("YR_TE_HIXL_ROUTE", K_DEFAULT_HIXL_ROUTE);
    std::transform(route.begin(), route.end(), route.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    TE_CHECK_OR_RETURN(IsSupportedRoute(route), ErrorCode::kInvalid,
                       "YR_TE_HIXL_ROUTE should be auto, hccs or roce");
    const char *forceRoce = std::getenv("HCCL_INTRA_ROCE_ENABLE");
    if (route == "hccs" && forceRoce != nullptr && std::string(forceRoce) == "1") {
        return TE_MAKE_STATUS(ErrorCode::kNotSupported,
                              "YR_TE_HIXL_ROUTE=hccs conflicts with HCCL_INTRA_ROCE_ENABLE=1");
    }
    *routePolicy = route;
    return Result::OK();
}

Result AscendBackend::BuildEndpoint(const std::string &localHost, int32_t localDeviceId, std::string *endpoint)
{
    TE_CHECK_PTR_OR_RETURN(endpoint);
    std::string explicitEndpoint;
    if (GetEnvIfSet("YR_TE_HIXL_ENDPOINT", explicitEndpoint)) {
        *endpoint = explicitEndpoint;
        return Result::OK();
    }

    int32_t basePort = GetEnvI32("YR_TE_HIXL_BASE_PORT", K_DEFAULT_HIXL_BASE_PORT);
    TE_CHECK_OR_RETURN(basePort > 0 && basePort <= K_MAX_TCP_PORT, ErrorCode::kInvalid, "invalid hixl base port");
    const int32_t physicalDeviceId = ResolvePhysicalDeviceId(localDeviceId);
    const int64_t segmentStart =
        static_cast<int64_t>(basePort) + static_cast<int64_t>(physicalDeviceId) * K_PORT_SEGMENT_SIZE;
    TE_CHECK_OR_RETURN(segmentStart > 0 && segmentStart <= K_MAX_TCP_PORT, ErrorCode::kInvalid,
                       "hixl endpoint port segment is out of range");
    for (int32_t offset = 0; offset < K_PORT_SEGMENT_SIZE && segmentStart + offset <= K_MAX_TCP_PORT; ++offset) {
        const uint16_t port = static_cast<uint16_t>(segmentStart + offset);
        int fd = -1;
        Result probeRc = CreateListenSocket(localHost, port, 1, fd, ListenSocketFailureLogLevel::kVlog1);
        if (probeRc.IsOk()) {
            if (fd >= 0) {
                (void)::close(fd);
            }
            *endpoint = localHost + ":" + std::to_string(port);
            return Result::OK();
        }
    }
    return TE_MAKE_STATUS(ErrorCode::kNotReady, "no available hixl endpoint port in device segment");
}

int32_t AscendBackend::GetEnvI32(const char *name, int32_t defaultValue)
{
    const char *env = std::getenv(name);
    if (env == nullptr || env[0] == '\0') {
        return defaultValue;
    }
    int64_t value = 0;
    for (const char *p = env; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') {
            TE_LOG_WARNING << "invalid integer environment value, using default, name=" << name;
            return defaultValue;
        }
        value = value * K_DECIMAL_BASE + static_cast<int64_t>(*p - '0');
        if (value > std::numeric_limits<int32_t>::max()) {
            TE_LOG_WARNING << "integer environment value is out of range, using default, name=" << name;
            return defaultValue;
        }
    }
    if (value <= 0) {
        TE_LOG_WARNING << "integer environment value should be positive, using default, name=" << name;
        return defaultValue;
    }
    return static_cast<int32_t>(value);
}

Result AscendBackend::ValidateBackingAlignmentLocked(uint64_t addr, uint64_t length) const
{
    if (routePolicy_ == "roce") {
        return Result::OK();
    }
    const uint64_t alignmentRemainder = addr % K_REGISTER_BASE_ALIGNMENT;
    if (alignmentRemainder == 0) {
        return Result::OK();
    }
    TE_LOG_ERROR << "reject unaligned memory backing before potential HCCS registration"
                 << ", addr=0x" << std::hex << addr << std::dec << ", length=" << length
                 << ", alignment_bytes=" << K_REGISTER_BASE_ALIGNMENT
                 << ", alignment_remainder=" << alignmentRemainder
                 << ", local_device_id=" << localDeviceId_ << ", hixl_route_policy=" << routePolicy_;
    return TE_MAKE_STATUS(ErrorCode::kInvalid,
                          "HCCS-safe HIXL registration requires a 2 MiB-aligned backing base; "
                          "transfer lengths remain byte-granular");
}

Result AscendBackend::RegisterOneLocked(uint64_t addr, uint64_t length, bool *registeredNew)
{
    TE_CHECK_PTR_OR_RETURN(registeredNew);
    *registeredNew = false;
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "ascend backend is not initialized");
    TE_CHECK_OR_RETURN(addr > 0 && length > 0, ErrorCode::kInvalid, "invalid hixl memory region");
    TE_RETURN_IF_ERROR(ValidateBackingAlignmentLocked(addr, length));
    for (const auto &entry : registeredMems_) {
        if (IsRangeInside(addr, length, entry.second.addr, entry.second.length)) {
            return Result::OK();
        }
    }
    auto existing = registeredMems_.find(addr);
    if (existing != registeredMems_.end()) {
        TE_CHECK_OR_RETURN(existing->second.addr == addr && existing->second.length == length, ErrorCode::kInvalid,
                           "hixl memory base already registered with different length");
        return Result::OK();
    }
    hixl::MemDesc desc{};
    desc.addr = static_cast<uintptr_t>(addr);
    desc.len = static_cast<size_t>(length);
    hixl::MemHandle handle = nullptr;
    const hixl::Status status = impl_->engine.RegisterMem(desc, hixl::MEM_DEVICE, handle);
    if (status != hixl::SUCCESS) {
        TE_LOG_ERROR << "hixl register memory failed"
                     << ", addr=0x" << std::hex << addr << std::dec << ", length=" << length << ", memory_type=device"
                     << ", local_device_id=" << localDeviceId_ << ", hixl_route_policy=" << routePolicy_
                     << ", hixl_status=" << status;
        return HixlStatusToResult(status, "Hixl::RegisterMem(MEM_DEVICE)");
    }
    RegisteredMem mem;
    mem.addr = addr;
    mem.length = length;
    mem.handle = handle;
    registeredMems_[addr] = mem;
    *registeredNew = true;
    TE_VLOG_1 << "hixl register memory success"
              << ", addr=0x" << std::hex << addr << std::dec
              << ", length=" << length
              << ", device_id=" << localDeviceId_;
    return Result::OK();
}

Result AscendBackend::UnregisterOneLocked(uint64_t addr, uint64_t length, bool failIfMissing, bool *unregistered)
{
    if (unregistered != nullptr) {
        *unregistered = false;
    }
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "ascend backend is not initialized");
    auto iter = registeredMems_.find(addr);
    if (iter == registeredMems_.end()) {
        if (failIfMissing) {
            return TE_MAKE_STATUS(ErrorCode::kNotFound, "hixl memory region is not registered");
        }
        return Result::OK();
    }
    TE_CHECK_OR_RETURN(iter->second.addr == addr && iter->second.length == length, ErrorCode::kInvalid,
                       "hixl unregister memory length mismatch");
    // HIXL rejects DeregisterMem while any client manager is still connected.
    TE_RETURN_IF_ERROR(DisconnectAllLocked());
    hixl::MemHandle handle = iter->second.handle;
    const hixl::Status status = impl_->engine.DeregisterMem(handle);
    if (status != hixl::SUCCESS) {
        TE_LOG_ERROR << "hixl unregister memory failed"
                     << ", addr=0x" << std::hex << addr << std::dec << ", length=" << length
                     << ", local_device_id=" << localDeviceId_ << ", hixl_route_policy=" << routePolicy_
                     << ", hixl_status=" << status;
        return HixlStatusToResult(status, "Hixl::DeregisterMem");
    }
    registeredMems_.erase(iter);
    if (unregistered != nullptr) {
        *unregistered = true;
    }
    TE_VLOG_1 << "hixl unregister memory success"
              << ", addr=0x" << std::hex << addr << std::dec
              << ", length=" << length
              << ", device_id=" << localDeviceId_;
    return Result::OK();
}

Result AscendBackend::DisconnectAllLocked()
{
    hixl::Status firstFailure = hixl::SUCCESS;
    for (auto iter = connectedEndpoints_.begin(); iter != connectedEndpoints_.end();) {
        const hixl::Status status = impl_->engine.Disconnect(hixl::AscendString(iter->c_str()), connectTimeoutMs_);
        LogDisconnectFailure(status, "Hixl::Disconnect during disconnect-all", *iter, connectTimeoutMs_);
        if (status == hixl::SUCCESS || status == hixl::NOT_CONNECTED) {
            iter = connectedEndpoints_.erase(iter);
            continue;
        }
        if (firstFailure == hixl::SUCCESS) {
            firstFailure = status;
        }
        ++iter;
    }
    peerEndpointByConnection_.clear();
    return HixlStatusToResult(firstFailure, "Hixl::Disconnect during disconnect-all");
}

Result AscendBackend::ConnectLocked(const std::string &connectionKey, const std::string &endpoint)
{
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "ascend backend is not initialized");
    auto existing = peerEndpointByConnection_.find(connectionKey);
    if (existing != peerEndpointByConnection_.end() && existing->second == endpoint &&
        connectedEndpoints_.find(endpoint) != connectedEndpoints_.end()) {
        return Result::OK();
    }
    if (autoConnectEnabled_) {
        peerEndpointByConnection_[connectionKey] = endpoint;
        connectedEndpoints_.insert(endpoint);
        TE_LOG_INFO << "hixl auto-connect route prepared"
                    << ", connection_key=" << connectionKey
                    << ", remote_hixl_endpoint=" << endpoint;
        return Result::OK();
    }
    TE_LOG_INFO << "hixl connect begin"
                << ", connection_key=" << connectionKey << ", local_hixl_endpoint=" << hixlEndpoint_
                << ", remote_hixl_endpoint=" << endpoint << ", hixl_route_policy=" << routePolicy_
                << ", connect_timeout_ms=" << connectTimeoutMs_;
    const hixl::Status status = impl_->engine.Connect(hixl::AscendString(endpoint.c_str()), connectTimeoutMs_);
    if (status != hixl::ALREADY_CONNECTED) {
        if (status != hixl::SUCCESS) {
            TE_LOG_ERROR << "hixl connect failed"
                         << ", connection_key=" << connectionKey << ", local_hixl_endpoint=" << hixlEndpoint_
                         << ", remote_hixl_endpoint=" << endpoint << ", hixl_route_policy=" << routePolicy_
                         << ", connect_timeout_ms=" << connectTimeoutMs_ << ", hixl_status=" << status;
            return HixlStatusToResult(status, "Hixl::Connect");
        }
    }
    peerEndpointByConnection_[connectionKey] = endpoint;
    connectedEndpoints_.insert(endpoint);
    TE_LOG_INFO << "hixl connect success"
                << ", remote_hixl_endpoint=" << endpoint
                << ", local_hixl_endpoint=" << hixlEndpoint_
                << ", connect_timeout_ms=" << connectTimeoutMs_;
    return Result::OK();
}

}  // namespace datasystem
