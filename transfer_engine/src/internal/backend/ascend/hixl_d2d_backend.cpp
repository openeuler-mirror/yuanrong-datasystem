/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */

#include "internal/backend/ascend/hixl_d2d_backend.h"

#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <map>
#include <sstream>
#include <utility>
#include <vector>

#include <hixl/hixl.h>

#include "internal/control_plane/socket_rpc_transport.h"
#include "internal/log/environment_dump.h"
#include "internal/log/logging.h"
#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace {

constexpr int32_t K_DEFAULT_HIXL_BASE_PORT = 20000;
constexpr int32_t K_PORT_SEGMENT_SIZE = 100;
constexpr int32_t K_DEFAULT_CONNECT_TIMEOUT_MS = 10000;
constexpr int32_t K_DEFAULT_TRANSFER_TIMEOUT_MS = 10000;
constexpr int32_t K_MAX_TCP_PORT = 65535;
constexpr int32_t K_DECIMAL_BASE = 10;
constexpr size_t K_MAX_TRANSFER_OPS_PER_CALL = 4096;
constexpr char K_OPTION_AUTO_CONNECT[] = "AutoConnect";
constexpr char K_OPTION_GLOBAL_RESOURCE_CONFIG[] = "GlobalResourceConfig";

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

}  // namespace

struct HixlD2DBackend::Impl {
    hixl::Hixl engine;
    bool initialized = false;
};

HixlD2DBackend::HixlD2DBackend() : impl_(std::make_unique<Impl>())
{
    internal::InitializeLogging();
}

HixlD2DBackend::~HixlD2DBackend()
{
    FinalizeLocal();
}

void HixlD2DBackend::FinalizeLocal()
{
    std::lock_guard<std::mutex> lock(mutex_);
    DisconnectAllLocked();
    for (auto &entry : registeredMems_) {
        if (entry.second.handle != nullptr) {
            (void)impl_->engine.DeregisterMem(entry.second.handle);
        }
    }
    registeredMems_.clear();
    if (impl_->initialized) {
        impl_->engine.Finalize();
        impl_->initialized = false;
    }
}

std::string HixlD2DBackend::RoutePolicy() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return routePolicy_;
}

uint64_t HixlD2DBackend::MemoryGeneration() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return memGeneration_;
}

Result HixlD2DBackend::InitializeLocal(const std::string &localHost, uint16_t localPort, int32_t localDeviceId)
{
    TE_CHECK_OR_RETURN(localDeviceId >= 0, ErrorCode::kInvalid, "local_device_id is invalid");
    std::string routePolicy;
    TE_RETURN_IF_ERROR(ParseRoutePolicy(&routePolicy));

    std::string endpoint;
    TE_RETURN_IF_ERROR(BuildEndpoint(localHost, localDeviceId, &endpoint));

    std::map<hixl::AscendString, hixl::AscendString> options;
    options[hixl::AscendString(hixl::OPTION_BUFFER_POOL)] =
        hixl::AscendString(GetEnvOrDefault("TRANSFER_ENGINE_HIXL_BUFFER_POOL", "0:0").c_str());

    std::string value;
    if (GetEnvIfSet("TRANSFER_ENGINE_HIXL_AUTO_CONNECT", value)) {
        options[hixl::AscendString(K_OPTION_AUTO_CONNECT)] = hixl::AscendString(value.c_str());
    }
    if (GetEnvIfSet("TRANSFER_ENGINE_HIXL_GLOBAL_RESOURCE_CONFIG", value)) {
        options[hixl::AscendString(K_OPTION_GLOBAL_RESOURCE_CONFIG)] = hixl::AscendString(value.c_str());
    }
    if (GetEnvIfSet("ASCEND_RDMA_TC", value) || GetEnvIfSet("HCCL_RDMA_TC", value)) {
        options[hixl::AscendString(hixl::OPTION_RDMA_TRAFFIC_CLASS)] = hixl::AscendString(value.c_str());
    }
    if (GetEnvIfSet("ASCEND_RDMA_SL", value) || GetEnvIfSet("HCCL_RDMA_SL", value)) {
        options[hixl::AscendString(hixl::OPTION_RDMA_SERVICE_LEVEL)] = hixl::AscendString(value.c_str());
    }

    connectTimeoutMs_ = GetEnvI32("TRANSFER_ENGINE_HIXL_CONNECT_TIMEOUT_MS", K_DEFAULT_CONNECT_TIMEOUT_MS);
    transferTimeoutMs_ = GetEnvI32("TRANSFER_ENGINE_HIXL_TRANSFER_TIMEOUT_MS", K_DEFAULT_TRANSFER_TIMEOUT_MS);

    std::lock_guard<std::mutex> lock(mutex_);
    TE_CHECK_OR_RETURN(!impl_->initialized, ErrorCode::kInvalid, "hixl backend already initialized");
    localDeviceId_ = localDeviceId;
    routePolicy_ = routePolicy;
    hixlEndpoint_ = endpoint;

    internal::DumpProcessEnvironment("hixl_backend_initialize");
    const hixl::Status status = impl_->engine.Initialize(hixl::AscendString(hixlEndpoint_.c_str()), options);
    TE_RETURN_IF_ERROR(HixlStatusToResult(status, "Hixl::Initialize"));
    impl_->initialized = true;

    TE_LOG_INFO << "hixl backend initialized"
                << ", local_host=" << localHost << ", local_port=" << localPort
                << ", local_device_id=" << localDeviceId_
                << ", hixl_endpoint=" << hixlEndpoint_
                << ", hixl_route_policy=" << routePolicy_
                << ", connect_timeout_ms=" << connectTimeoutMs_
                << ", transfer_timeout_ms=" << transferTimeoutMs_;
    return Result::OK();
}

Result HixlD2DBackend::RegisterLocalMemory(uint64_t addr, uint64_t length)
{
    TE_CHECK_OR_RETURN(addr > 0 && length > 0, ErrorCode::kInvalid, "invalid memory region");
    std::lock_guard<std::mutex> lock(mutex_);
    bool registeredNew = false;
    TE_RETURN_IF_ERROR(RegisterOneLocked(addr, length, &registeredNew));
    if (registeredNew) {
        ++memGeneration_;
        DisconnectAllLocked();
    }
    return Result::OK();
}

Result HixlD2DBackend::UnregisterLocalMemory(uint64_t addr, uint64_t length)
{
    TE_CHECK_OR_RETURN(addr > 0 && length > 0, ErrorCode::kInvalid, "invalid memory region");
    std::lock_guard<std::mutex> lock(mutex_);
    bool unregistered = false;
    Result rc = UnregisterOneLocked(addr, length, true, &unregistered);
    if (unregistered) {
        ++memGeneration_;
        DisconnectAllLocked();
    }
    return rc;
}

Result HixlD2DBackend::PrepareReadDestinations(const std::vector<TransferMemoryRegion> &regions)
{
    std::lock_guard<std::mutex> lock(mutex_);
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "hixl backend is not initialized");
    for (const auto &region : regions) {
        TE_CHECK_OR_RETURN(region.addr > 0 && region.length > 0, ErrorCode::kInvalid,
                           "invalid hixl read destination");
        bool registered = false;
        for (const auto &entry : registeredMems_) {
            if (IsRangeInside(region.addr, region.length, entry.second.addr, entry.second.length)) {
                registered = true;
                break;
            }
        }
        TE_CHECK_OR_RETURN(registered, ErrorCode::kNotFound, "hixl read destination memory is not registered");
    }
    return Result::OK();
}

Result HixlD2DBackend::CreateRootInfo(std::string *rootInfoBytes)
{
    TE_CHECK_PTR_OR_RETURN(rootInfoBytes);
    std::lock_guard<std::mutex> lock(mutex_);
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "hixl backend is not initialized");
    RootInfo info;
    info.backendKind = BackendKind();
    info.endpoint = hixlEndpoint_;
    info.routePolicy = routePolicy_;
    *rootInfoBytes = EncodeRootInfo(info);
    return Result::OK();
}

Result HixlD2DBackend::InitRecv(const ConnectionSpec &spec, const std::string &rootInfoBytes)
{
    RootInfo rootInfo;
    TE_RETURN_IF_ERROR(ParseRootInfo(rootInfoBytes, &rootInfo));
    TE_CHECK_OR_RETURN(rootInfo.backendKind == BackendKind(), ErrorCode::kNotSupported,
                       "peer root info is not hixl");
    TE_CHECK_OR_RETURN(rootInfo.routePolicy == routePolicy_, ErrorCode::kNotSupported,
                       "hixl route policy mismatch");
    TE_CHECK_OR_RETURN(!rootInfo.endpoint.empty(), ErrorCode::kInvalid, "hixl peer endpoint is empty");

    std::lock_guard<std::mutex> lock(mutex_);
    TE_RETURN_IF_ERROR(ConnectLocked(ConnectionKey(spec), rootInfo.endpoint));
    return Result::OK();
}

Result HixlD2DBackend::InitSend(const ConnectionSpec &spec, const std::string &rootInfoBytes)
{
    RootInfo rootInfo;
    Result parseRc = ParseRootInfo(rootInfoBytes, &rootInfo);
    if (parseRc.IsError()) {
        return parseRc;
    }
    TE_LOG_INFO << "hixl owner accepted requester root info"
                << ", requester=" << spec.peerHost << ":" << spec.peerPort
                << ", requester_device_id=" << spec.peerDeviceId
                << ", requester_hixl_endpoint=" << rootInfo.endpoint;
    return Result::OK();
}

Result HixlD2DBackend::PostRecv(const ConnectionSpec &spec, uint64_t localAddr, uint64_t length)
{
    (void)spec;
    (void)localAddr;
    (void)length;
    return Result(ErrorCode::kNotSupported, "hixl backend uses receiver-driven read");
}

Result HixlD2DBackend::PostSend(const ConnectionSpec &spec, uint64_t remoteAddr, uint64_t length)
{
    (void)spec;
    (void)remoteAddr;
    (void)length;
    return Result(ErrorCode::kNotSupported, "hixl backend uses receiver-driven read");
}

Result HixlD2DBackend::WaitRecv(const ConnectionSpec &spec, uint64_t timeoutMs)
{
    (void)spec;
    (void)timeoutMs;
    return Result(ErrorCode::kNotSupported, "hixl backend uses receiver-driven read");
}

Result HixlD2DBackend::TransferSyncRead(const ConnectionSpec &spec, const std::vector<TransferReadOp> &ops,
                                        uint64_t timeoutMs)
{
    TE_CHECK_OR_RETURN(!ops.empty(), ErrorCode::kInvalid, "read ops is empty");
    std::lock_guard<std::mutex> lock(mutex_);
    const auto iter = peerEndpointByConnection_.find(ConnectionKey(spec));
    TE_CHECK_OR_RETURN(iter != peerEndpointByConnection_.end(), ErrorCode::kNotReady, "hixl connection not found");
    const std::string endpoint = iter->second;

    for (size_t base = 0; base < ops.size(); base += K_MAX_TRANSFER_OPS_PER_CALL) {
        const size_t end = std::min(base + K_MAX_TRANSFER_OPS_PER_CALL, ops.size());
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
        const hixl::Status status =
            impl_->engine.TransferSync(hixl::AscendString(endpoint.c_str()), hixl::READ, descs, effectiveTimeout);
        Result rc = HixlStatusToResult(status, "Hixl::TransferSync(READ)");
        if (rc.IsError()) {
            (void)impl_->engine.Disconnect(hixl::AscendString(endpoint.c_str()), connectTimeoutMs_);
            connectedEndpoints_.erase(endpoint);
            peerEndpointByConnection_.erase(iter);
            return rc;
        }
    }
    TE_VLOG_1 << "hixl transfer sync read success"
                << ", peer=" << spec.peerHost << ":" << spec.peerPort
                << ", peer_device_id=" << spec.peerDeviceId
                << ", hixl_endpoint=" << endpoint
                << ", op_count=" << ops.size();
    return Result::OK();
}

void HixlD2DBackend::AbortConnection(const ConnectionSpec &spec)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = ConnectionKey(spec);
    auto iter = peerEndpointByConnection_.find(key);
    if (iter == peerEndpointByConnection_.end()) {
        return;
    }
    const std::string endpoint = iter->second;
    (void)impl_->engine.Disconnect(hixl::AscendString(endpoint.c_str()), connectTimeoutMs_);
    connectedEndpoints_.erase(endpoint);
    peerEndpointByConnection_.erase(iter);
}

std::string HixlD2DBackend::ConnectionKey(const ConnectionSpec &spec)
{
    return spec.localHost + ":" + std::to_string(spec.localPort) + ":" + std::to_string(spec.localDeviceId) + "|" +
           spec.peerHost + ":" + std::to_string(spec.peerPort) + ":" + std::to_string(spec.peerDeviceId);
}

Result HixlD2DBackend::ParseRootInfo(const std::string &rootInfoBytes, RootInfo *rootInfo)
{
    TE_CHECK_PTR_OR_RETURN(rootInfo);
    std::istringstream input(rootInfoBytes);
    std::string line;
    if (!std::getline(input, line) || line != "transfer_engine_hixl_root_info_v1") {
        return TE_MAKE_STATUS(ErrorCode::kInvalid, "invalid hixl root info header");
    }
    while (std::getline(input, line)) {
        const size_t sep = line.find('=');
        if (sep == std::string::npos) {
            continue;
        }
        const std::string key = line.substr(0, sep);
        const std::string value = line.substr(sep + 1);
        if (key == "backend") {
            rootInfo->backendKind = value;
        } else if (key == "endpoint") {
            rootInfo->endpoint = value;
        } else if (key == "route") {
            rootInfo->routePolicy = value;
        }
    }
    TE_CHECK_OR_RETURN(rootInfo->backendKind == "hixl", ErrorCode::kInvalid, "invalid hixl root backend");
    TE_CHECK_OR_RETURN(!rootInfo->endpoint.empty(), ErrorCode::kInvalid, "hixl root endpoint is empty");
    TE_CHECK_OR_RETURN(IsSupportedRoute(rootInfo->routePolicy), ErrorCode::kInvalid, "invalid hixl route policy");
    return Result::OK();
}

std::string HixlD2DBackend::EncodeRootInfo(const RootInfo &rootInfo)
{
    std::ostringstream out;
    out << "transfer_engine_hixl_root_info_v1\n"
        << "backend=" << rootInfo.backendKind << "\n"
        << "endpoint=" << rootInfo.endpoint << "\n"
        << "route=" << rootInfo.routePolicy << "\n";
    return out.str();
}

Result HixlD2DBackend::ParseRoutePolicy(std::string *routePolicy)
{
    TE_CHECK_PTR_OR_RETURN(routePolicy);
    std::string route = GetEnvOrDefault("TRANSFER_ENGINE_HIXL_ROUTE", "auto");
    std::transform(route.begin(), route.end(), route.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    TE_CHECK_OR_RETURN(IsSupportedRoute(route), ErrorCode::kInvalid,
                       "TRANSFER_ENGINE_HIXL_ROUTE should be auto, hccs or roce");
    const char *forceRoce = std::getenv("HCCL_INTRA_ROCE_ENABLE");
    if (route == "hccs" && forceRoce != nullptr && std::string(forceRoce) == "1") {
        return TE_MAKE_STATUS(ErrorCode::kNotSupported,
                              "TRANSFER_ENGINE_HIXL_ROUTE=hccs conflicts with HCCL_INTRA_ROCE_ENABLE=1");
    }
    *routePolicy = route;
    return Result::OK();
}

Result HixlD2DBackend::BuildEndpoint(const std::string &localHost, int32_t localDeviceId, std::string *endpoint)
{
    TE_CHECK_PTR_OR_RETURN(endpoint);
    std::string explicitEndpoint;
    if (GetEnvIfSet("TRANSFER_ENGINE_HIXL_ENDPOINT", explicitEndpoint)) {
        *endpoint = explicitEndpoint;
        return Result::OK();
    }

    int32_t basePort = GetEnvI32("TRANSFER_ENGINE_HIXL_BASE_PORT", -1);
    if (basePort < 0) {
        basePort = GetEnvI32("ASCEND_BASE_PORT", K_DEFAULT_HIXL_BASE_PORT);
    }
    TE_CHECK_OR_RETURN(basePort > 0 && basePort <= K_MAX_TCP_PORT, ErrorCode::kInvalid, "invalid hixl base port");
    const int32_t physicalDeviceId = ResolvePhysicalDeviceId(localDeviceId);
    const int64_t segmentStart =
        static_cast<int64_t>(basePort) + static_cast<int64_t>(physicalDeviceId) * K_PORT_SEGMENT_SIZE;
    TE_CHECK_OR_RETURN(segmentStart > 0 && segmentStart <= K_MAX_TCP_PORT, ErrorCode::kInvalid,
                       "hixl endpoint port segment is out of range");
    for (int32_t offset = 0; offset < K_PORT_SEGMENT_SIZE && segmentStart + offset <= K_MAX_TCP_PORT; ++offset) {
        const uint16_t port = static_cast<uint16_t>(segmentStart + offset);
        int fd = -1;
        Result probeRc = CreateListenSocket(localHost, port, 1, &fd);
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

int32_t HixlD2DBackend::GetEnvI32(const char *name, int32_t defaultValue)
{
    const char *env = std::getenv(name);
    if (env == nullptr || env[0] == '\0') {
        return defaultValue;
    }
    int64_t value = 0;
    for (const char *p = env; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') {
            return defaultValue;
        }
        value = value * K_DECIMAL_BASE + static_cast<int64_t>(*p - '0');
        if (value > std::numeric_limits<int32_t>::max()) {
            return defaultValue;
        }
    }
    return static_cast<int32_t>(value);
}

Result HixlD2DBackend::RegisterOneLocked(uint64_t addr, uint64_t length, bool *registeredNew)
{
    TE_CHECK_PTR_OR_RETURN(registeredNew);
    *registeredNew = false;
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "hixl backend is not initialized");
    TE_CHECK_OR_RETURN(addr > 0 && length > 0, ErrorCode::kInvalid, "invalid hixl memory region");
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
    TE_RETURN_IF_ERROR(HixlStatusToResult(impl_->engine.RegisterMem(desc, hixl::MEM_DEVICE, handle),
                                          "Hixl::RegisterMem(MEM_DEVICE)"));
    RegisteredMem mem;
    mem.addr = addr;
    mem.length = length;
    mem.handle = handle;
    registeredMems_[addr] = mem;
    *registeredNew = true;
    TE_LOG_INFO << "hixl register memory success"
                << ", addr=0x" << std::hex << addr << std::dec
                << ", length=" << length
                << ", device_id=" << localDeviceId_;
    return Result::OK();
}

Result HixlD2DBackend::UnregisterOneLocked(uint64_t addr, uint64_t length, bool failIfMissing, bool *unregistered)
{
    if (unregistered != nullptr) {
        *unregistered = false;
    }
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "hixl backend is not initialized");
    auto iter = registeredMems_.find(addr);
    if (iter == registeredMems_.end()) {
        if (failIfMissing) {
            return TE_MAKE_STATUS(ErrorCode::kNotFound, "hixl memory region is not registered");
        }
        return Result::OK();
    }
    TE_CHECK_OR_RETURN(iter->second.addr == addr && iter->second.length == length, ErrorCode::kInvalid,
                       "hixl unregister memory length mismatch");
    hixl::MemHandle handle = iter->second.handle;
    TE_RETURN_IF_ERROR(HixlStatusToResult(impl_->engine.DeregisterMem(handle), "Hixl::DeregisterMem"));
    registeredMems_.erase(iter);
    if (unregistered != nullptr) {
        *unregistered = true;
    }
    TE_LOG_INFO << "hixl unregister memory success"
                << ", addr=0x" << std::hex << addr << std::dec
                << ", length=" << length
                << ", device_id=" << localDeviceId_;
    return Result::OK();
}

void HixlD2DBackend::DisconnectAllLocked()
{
    for (const auto &endpoint : connectedEndpoints_) {
        (void)impl_->engine.Disconnect(hixl::AscendString(endpoint.c_str()), connectTimeoutMs_);
    }
    connectedEndpoints_.clear();
    peerEndpointByConnection_.clear();
}

Result HixlD2DBackend::ConnectLocked(const std::string &connectionKey, const std::string &endpoint)
{
    TE_CHECK_OR_RETURN(impl_->initialized, ErrorCode::kNotReady, "hixl backend is not initialized");
    auto existing = peerEndpointByConnection_.find(connectionKey);
    if (existing != peerEndpointByConnection_.end() && existing->second == endpoint &&
        connectedEndpoints_.find(endpoint) != connectedEndpoints_.end()) {
        return Result::OK();
    }
    const hixl::Status status = impl_->engine.Connect(hixl::AscendString(endpoint.c_str()), connectTimeoutMs_);
    if (status != hixl::ALREADY_CONNECTED) {
        TE_RETURN_IF_ERROR(HixlStatusToResult(status, "Hixl::Connect"));
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
