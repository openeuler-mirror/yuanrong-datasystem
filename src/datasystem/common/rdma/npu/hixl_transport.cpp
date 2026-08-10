/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

#include "datasystem/common/rdma/npu/hixl_transport.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <netdb.h>
#include <netinet/in.h>
#include <random>
#include <securec.h>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/rdma/npu/hixl_plugin_loader.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_string(remote_h2d_hccs_buffer_pool);
DS_DECLARE_bool(hixl_cs_enable);

namespace {

// Fixed device-NIC listen port for worker one-sided comm. Each worker device has its own
// device IP, so the same port across devices does not collide.
constexpr int K_WORKER_DEVICE_COMM_PORT = 26666;
#ifdef WITH_TESTS
constexpr int K_FAKE_HIXL_TEST_PORT = 26667;
#endif
constexpr int K_EPHEMERAL_PORT_START = 49152;
constexpr int K_EPHEMERAL_PORT_END = 65535;
constexpr int K_MAX_PORT_ALLOC_ATTEMPTS = 5;
constexpr int32_t K_HIXL_CONNECT_TIMEOUT_MS = 30 * 1000;
constexpr int32_t K_HIXL_DISCONNECT_TIMEOUT_MS = 1000;
constexpr int32_t K_HIXL_TRANSFER_TIMEOUT_MS = 10 * 1000;
const std::string K_HCCL_INTRA_ROCE_ENABLE = "HCCL_INTRA_ROCE_ENABLE";
const std::string K_HIXL_DIRECT_ROCE_BUFFER_POOL = "0:0";
const std::string K_HIXL_OPTION_BUFFER_POOL = "BufferPool";
const std::string K_HIXL_OPTION_LOCAL_COMM_RES = "LocalCommRes";
const std::string K_HIXL_CS_LOCAL_COMM_RES = R"({"version":"1.3"})";
const std::string K_HIXL_OPTION_GLOBAL_RESOURCE_CONFIG = "GlobalResourceConfig";

// HCCL registration limit: keep a small reserve under the 256 MEM_DEVICE limit.
// Long-lived pre-registrations and temporary fallback registrations share this budget.
constexpr size_t MAX_DEVICE_REGISTRATIONS = 253;

datasystem::HixlMemoryMode DetermineHixlMemoryMode()
{
    // FabricMem mode can be inserted here once its worker/client flag is available in this branch.
    if (datasystem::GetBoolFromEnv(K_HCCL_INTRA_ROCE_ENABLE.c_str(), false)) {
        return datasystem::HixlMemoryMode::ROCE_DIRECT;
    }
    return datasystem::HixlMemoryMode::BUFFER_POOL;
}

std::string HixlMemoryModeName(datasystem::HixlMemoryMode mode)
{
    switch (mode) {
        case datasystem::HixlMemoryMode::ROCE_DIRECT:
            return "roce_direct";
        case datasystem::HixlMemoryMode::FABRIC_MEM:
            return "fabric_mem";
        case datasystem::HixlMemoryMode::BUFFER_POOL:
        default:
            return "buffer_pool";
    }
}

datasystem::Status HixlResultToStatus(DsHixlResult result, uint32_t vendorStatus, const std::string &operation)
{
    if (result == DS_HIXL_OK) {
        return datasystem::Status::OK();
    }
    const std::string message =
        datasystem::FormatString("HIXL plugin %s failed, result=%d, vendor_status=%u", operation, result, vendorStatus);
    if (result == DS_HIXL_INVALID_ARGUMENT) {
        return datasystem::Status(datasystem::StatusCode::K_INVALID, message);
    }
    if (result == DS_HIXL_NOT_SUPPORTED) {
        return datasystem::Status(datasystem::StatusCode::K_NOT_SUPPORTED, message);
    }
    return datasystem::Status(datasystem::StatusCode::K_RUNTIME_ERROR, message);
}

// Allocate a random available ephemeral port. Tries up to K_MAX_PORT_ALLOC_ATTEMPTS times.
// Returns 0 if all attempts fail.
int AllocateRandomPort()
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(K_EPHEMERAL_PORT_START, K_EPHEMERAL_PORT_END);  // IANA ephemeral ports

    for (int attempt = 0; attempt < K_MAX_PORT_ALLOC_ATTEMPTS; ++attempt) {
        int port = dist(gen);
        std::string portStr = std::to_string(port);
        addrinfo hints{};
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;

        addrinfo *addr = nullptr;
        if (getaddrinfo(nullptr, portStr.c_str(), &hints, &addr) != 0 || addr == nullptr) {
            continue;
        }

        int sock = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (sock < 0) {
            freeaddrinfo(addr);
            continue;
        }

        if (bind(sock, addr->ai_addr, addr->ai_addrlen) == 0) {
            close(sock);
            freeaddrinfo(addr);
            return port;
        }
        close(sock);
        freeaddrinfo(addr);
    }

    return 0;  // all attempts failed
}

std::vector<std::pair<std::string, std::string>> BuildHixlOptions(const std::string &bufferPool, bool isClient)
{
    std::vector<std::pair<std::string, std::string>> optionValues;
    optionValues.emplace_back(K_HIXL_OPTION_BUFFER_POOL, bufferPool);
    if (FLAGS_hixl_cs_enable) {
        optionValues.emplace_back(K_HIXL_OPTION_LOCAL_COMM_RES, K_HIXL_CS_LOCAL_COMM_RES);
        LOG(INFO) << "[HCCS] HIXL CS enabled with LocalCommRes version 1.3";
    }
    if (!isClient) {
        std::string resourceConfig =
            R"({"comm_resource_config.listen_port":")" + std::to_string(K_WORKER_DEVICE_COMM_PORT) + R"("})";
        optionValues.emplace_back(K_HIXL_OPTION_GLOBAL_RESOURCE_CONFIG, std::move(resourceConfig));
    }
    return optionValues;
}

}  // namespace

namespace datasystem {

HixlTransport::~HixlTransport()
{
    LOG_IF_ERROR(DisconnectAll(), "Failed to release HIXL transport");
}

void HixlTransport::SetLocalEndpoint(const std::string &ep, bool isClient)
{
    localIp_ = ep;
    isClient_ = isClient;
}

bool HixlTransport::IsHixlRoceDirectMode() const
{
    return hixlMemoryMode_ == HixlMemoryMode::ROCE_DIRECT;
}

void HixlTransport::DestroyEngine(DsHixlEngineHandle engine) const
{
    if (api_ == nullptr || engine == nullptr) {
        return;
    }
    DsHixlResult finalizeResult = api_->finalize_engine(engine);
    LOG_IF(WARNING, finalizeResult != DS_HIXL_OK)
        << "HIXL plugin finalize_engine failed, result=" << finalizeResult;
    DsHixlResult destroyResult = api_->destroy_engine(engine);
    LOG_IF(WARNING, destroyResult != DS_HIXL_OK)
        << "HIXL plugin destroy_engine failed, result=" << destroyResult;
}

Status HixlTransport::InitializeSingleDevice(int32_t devId, const std::string &bufferPool,
                                             DsHixlEngineHandle &engine, std::string &endpoint)
{
    engine = nullptr;
#ifdef WITH_TESTS
    if (!skipDeviceBinding_) {
#endif
    Status setDevSt = acl::AclDeviceManager::Instance()->SetDeviceIdx(devId);
    if (setDevSt.IsError()) {
        LOG(WARNING) << "[HCCS] SetDeviceIdx failed for devId " << devId << ": " << setDevSt.GetMsg();
        return setDevSt;
    }
#ifdef WITH_TESTS
    }
#endif

    int port = 0;
#ifdef WITH_TESTS
    if (skipDeviceBinding_) {
        // The fake test API does not bind a socket. Keep the test deterministic and avoid consuming a host port.
        port = K_FAKE_HIXL_TEST_PORT;
    } else {
#endif
        port = AllocateRandomPort();
#ifdef WITH_TESTS
    }
#endif
    if (port <= 0) {
        LOG(WARNING) << "[HCCS] Failed to allocate random port for devId " << devId;
        return Status(StatusCode::K_RUNTIME_ERROR, "Failed to allocate port for at least one device");
    }

    endpoint = localIp_ + ":" + std::to_string(port);
    LOG(INFO) << "[HCCS] Hixl::Initialize start devId=" << devId << " endpoint=" << endpoint;
    RETURN_IF_NOT_OK(HixlResultToStatus(api_->create_engine(&engine), 0, "create_engine"));
    bool needRollback = true;
    Raii rollback([this, &engine, &needRollback]() {
        if (needRollback) {
            DestroyEngine(engine);
            engine = nullptr;
        }
    });

    auto optionValues = BuildHixlOptions(bufferPool, isClient_);
    std::vector<DsHixlOption> options;
    options.reserve(optionValues.size());
    for (const auto &option : optionValues) {
        options.push_back(DsHixlOption{ { option.first.data(), option.first.size() },
                                        { option.second.data(), option.second.size() } });
    }

    uint32_t vendorStatus = 0;
    DsHixlResult result = api_->initialize_engine(
        engine, DsHixlStringView{ endpoint.data(), endpoint.size() }, options.data(), options.size(), &vendorStatus);
    RETURN_IF_NOT_OK(HixlResultToStatus(result, vendorStatus, "initialize_engine"));
    needRollback = false;
    LOG(INFO) << "[HCCS] Engine created for devId " << devId << " at " << endpoint;
    return Status::OK();
}

Status HixlTransport::Init(const std::vector<int32_t> &deviceIds)
{
    LOG(INFO) << "[HCCS] Init localIp=" << localIp_ << " numDevices=" << deviceIds.size();
    RETURN_OK_IF_TRUE(initialized_);
    CHECK_FAIL_RETURN_STATUS(!localIp_.empty(), StatusCode::K_INVALID,
                             "HCCS local IP not configured. Call SetLocalEndpoint()");
    if (api_ == nullptr) {
        RETURN_IF_NOT_OK(HixlPluginLoader::Instance().GetApi(api_));
    }

    std::vector<int32_t> targetDevIds;
    if (!deviceIds.empty()) {
        targetDevIds = deviceIds;
    } else {
        int32_t singleDevId = -1;
        RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->GetDeviceIdx(singleDevId));
        targetDevIds.push_back(singleDevId);
    }

    hixlMemoryMode_ = DetermineHixlMemoryMode();
    const std::string bufferPool =
        IsHixlRoceDirectMode() ? K_HIXL_DIRECT_ROCE_BUFFER_POOL : FLAGS_remote_h2d_hccs_buffer_pool;
    std::map<int32_t, DsHixlEngineHandle> pendingEngines;
    std::map<int32_t, std::string> pendingEndpoints;
    std::vector<DsHixlEngineHandle> pendingOrder;
    pendingOrder.reserve(targetDevIds.size());
    bool needRollback = true;
    Raii rollback([this, &pendingOrder, &needRollback]() {
        if (needRollback) {
            for (auto iter = pendingOrder.rbegin(); iter != pendingOrder.rend(); ++iter) {
                DestroyEngine(*iter);
            }
        }
    });
    for (int32_t devId : targetDevIds) {
        CHECK_FAIL_RETURN_STATUS(pendingEngines.find(devId) == pendingEngines.end(), K_INVALID,
                                 "Duplicate HCCS device id: " + std::to_string(devId));
        DsHixlEngineHandle engine = nullptr;
        std::string endpoint;
        RETURN_IF_NOT_OK(InitializeSingleDevice(devId, bufferPool, engine, endpoint));
        // Record ownership before map insertion so allocation failures still destroy this engine.
        pendingOrder.emplace_back(engine);
        pendingEngines.emplace(devId, engine);
        pendingEndpoints.emplace(devId, std::move(endpoint));
    }

    engines_ = std::move(pendingEngines);
    localEndpointById_ = std::move(pendingEndpoints);
    initialized_ = true;
    needRollback = false;
    LOG(INFO) << "[HCCS] Initialized with " << engines_.size() << " engine(s) on IP " << localIp_
              << " with hixl memory mode: " << HixlMemoryModeName(hixlMemoryMode_)
              << ", buffer pool config: " << bufferPool;
    return Status::OK();
}

Status HixlTransport::GetConnectionIdentity(std::string *identity)
{
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HixlTransport not initialized");
    CHECK_FAIL_RETURN_STATUS(!engines_.empty(), StatusCode::K_RUNTIME_ERROR, "No HCCS engines available");

    // Round-robin across engines so each new p2p connection is distributed
    // across different device IDs and, therefore, different NPU cards.
    unsigned int idx = nextEngineIndex_.fetch_add(1, std::memory_order_relaxed);
    auto it = engines_.begin();
    std::advance(it, idx % engines_.size());
    *identity = localEndpointById_[it->first];
    return Status::OK();
}

Status HixlTransport::Connect(const std::string &remoteIdentity, P2pKind kind, std::function<int()> *heartbeatCallback)
{
    (void)heartbeatCallback;
    LOG(INFO) << "[HCCS] Connect enter, remote=" << remoteIdentity
              << " kind=" << (kind == P2P_SENDER ? "SENDER" : "RECEIVER");
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HixlTransport not initialized");

    // Sender just listens (Init set up the HIXL engine); only receiver calls Hixl::Connect.
    if (kind == P2P_SENDER) {
        LOG(INFO) << "[HCCS] Sender is listen-only, skipping Hixl::Connect";
        return Status::OK();
    }

    if (remoteIdentity.empty() || !Validator::ValidateHostPortString("HCCSEndpoint", remoteIdentity, true)) {
        return Status(StatusCode::K_INVALID, "Invalid HCCS remote endpoint: " + remoteIdentity);
    }
    HostPort remoteEndpoint;
    RETURN_IF_NOT_OK(remoteEndpoint.ParseString(remoteIdentity));
    CHECK_FAIL_RETURN_STATUS(remoteEndpoint.Port() > 0, StatusCode::K_INVALID,
                             "Invalid HCCS remote endpoint port: " + remoteIdentity);

    CHECK_FAIL_RETURN_STATUS(!engines_.empty(), StatusCode::K_RUNTIME_ERROR, "No HCCS engines available");
    DsHixlEngineHandle engine = engines_.begin()->second;

    {
        std::lock_guard<std::mutex> lock(connMutex_);
        if (activeEndpoints_.find(remoteIdentity) != activeEndpoints_.end()) {
            LOG(INFO) << "[HCCS] Already connected to " << remoteIdentity << ", skipping";
            return Status::OK();
        }

        // One HIXL engine can manage connections to multiple remote endpoints.
        LOG(INFO) << "[HCCS] Connecting from local endpoint to " << remoteIdentity;

        uint32_t vendorStatus = 0;
        DsHixlResult result = api_->connect_engine(
            engine, DsHixlStringView{ remoteIdentity.data(), remoteIdentity.size() }, K_HIXL_CONNECT_TIMEOUT_MS,
            &vendorStatus);
        RETURN_IF_NOT_OK(HixlResultToStatus(result, vendorStatus, "connect_engine"));

        activeEndpoints_.insert(remoteIdentity);
        LOG(INFO) << "[HCCS] Connected to " << remoteIdentity;
    }
    return Status::OK();
}

Status HixlTransport::Disconnect(const std::string &remoteIdentity)
{
    std::lock_guard<std::mutex> lock(connMutex_);
    RETURN_OK_IF_TRUE(activeEndpoints_.erase(remoteIdentity) == 0);

    // Use the first engine, consistent with Connect and ScatterBatch
    auto eng = engines_.begin();
    if (eng != engines_.end()) {
        uint32_t vendorStatus = 0;
        DsHixlResult result = api_->disconnect_engine(
            eng->second, DsHixlStringView{ remoteIdentity.data(), remoteIdentity.size() },
            K_HIXL_DISCONNECT_TIMEOUT_MS, &vendorStatus);
        RETURN_IF_NOT_OK(HixlResultToStatus(result, vendorStatus, "disconnect_engine"));
        LOG(INFO) << "[HCCS] Disconnected from " + remoteIdentity;
    }

    return Status::OK();
}

Status HixlTransport::DisconnectAll()
{
    if (!initialized_)
        return Status::OK();

    // Use the first engine, consistent with Connect and ScatterBatch
    auto eng = engines_.begin();
    if (eng != engines_.end()) {
        std::lock_guard<std::mutex> lock(connMutex_);
        for (const auto &remoteId : activeEndpoints_) {
            uint32_t vendorStatus = 0;
            DsHixlResult result = api_->disconnect_engine(
                eng->second, DsHixlStringView{ remoteId.data(), remoteId.size() }, K_HIXL_DISCONNECT_TIMEOUT_MS,
                &vendorStatus);
            if (result != DS_HIXL_OK) {
                LOG(WARNING) << "[HCCS] Failed to disconnect from " << remoteId << ", result=" << result
                             << ", vendor_status=" << vendorStatus;
            } else {
                LOG(INFO) << "[HCCS] Disconnected from " + remoteId;
            }
        }
        activeEndpoints_.clear();
    }

    ClearRegisteredDeviceMemory();
    ClearRegisteredHostMemory();

    for (const auto &[devId, engine] : engines_) {
        (void)devId;
        DestroyEngine(engine);
    }
    engines_.clear();
    localEndpointById_.clear();
    hixlMemoryMode_ = HixlMemoryMode::BUFFER_POOL;
    initialized_ = false;
    return Status::OK();
}

Status HixlTransport::RegisterMemory(void *addr, uint64_t size, P2pSegmentInfo *segInfo)
{
    if (segInfo != nullptr) {
        int ret = memset_s(segInfo, sizeof(*segInfo), 0, sizeof(*segInfo));
        CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR, "Failed to clear HCCS segment info");
    }

    // HCCS buffer-pool RH2D intentionally does not register the remote source host buffer. The source address
    // is carried inline by P2pScatterEntry::ddrBuf at transfer time; HIXL routes it through its internal
    // buffer-pool relay. HIXL ROCE direct mode requires the worker host buffer to be registered before Connect.
    RETURN_OK_IF_TRUE(!IsHixlRoceDirectMode());
    RETURN_OK_IF_TRUE(isClient_);
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HixlTransport not initialized");
    CHECK_FAIL_RETURN_STATUS(addr != nullptr, StatusCode::K_INVALID, "HCCS host memory address cannot be null");
    CHECK_FAIL_RETURN_STATUS(size > 0, StatusCode::K_INVALID, "HCCS host memory size must be greater than 0");

    int32_t devId = -1;
    RETURN_IF_NOT_OK(acl::AclDeviceManager::Instance()->GetDeviceIdx(devId));

    std::lock_guard<std::mutex> lock(transferMutex_);
    uintptr_t hostAddr = reinterpret_cast<uintptr_t>(addr);
    RETURN_OK_IF_TRUE(HasRegisteredHostMemoryLocked(devId, hostAddr, size));

    auto engineIter = engines_.find(devId);
    CHECK_FAIL_RETURN_STATUS(engineIter != engines_.end(), StatusCode::K_RUNTIME_ERROR,
                             "No HCCS engine available for devId " + std::to_string(devId));

    DsHixlMemHandle handle = nullptr;
    uint32_t vendorStatus = 0;
    DsHixlRegisterMemoryRequest request{ hostAddr, size, DS_HIXL_MEMORY_HOST };
    DsHixlResult result = api_->register_memory(engineIter->second, &request, &handle, &vendorStatus);
    RETURN_IF_NOT_OK(HixlResultToStatus(result, vendorStatus, "register_memory(MEM_HOST)"));
    registeredHostMemories_.push_back(RegisteredHostMemory{ devId, hostAddr, size, handle });
    return Status::OK();
}

bool HixlTransport::HasRegisteredHostMemoryLocked(int32_t devId, uintptr_t addr, uint64_t size) const
{
    return std::any_of(registeredHostMemories_.begin(), registeredHostMemories_.end(),
                       [devId, addr, size](const RegisteredHostMemory &registered) {
                           return registered.devId == devId && registered.addr <= addr && size <= registered.size
                                  && addr - registered.addr <= registered.size - size;
                        });
}

bool HixlTransport::HasRegisteredDeviceMemoryLocked(uintptr_t addr, uint64_t size) const
{
    return std::any_of(registeredDeviceMemories_.begin(), registeredDeviceMemories_.end(),
                       [addr, size](const RegisteredDeviceMemory &registered) {
                           return registered.addr <= addr && size <= registered.size
                                  && addr - registered.addr <= registered.size - size;
                        });
}

Status HixlTransport::RegisterDeviceMemoryLocked(uintptr_t addr, uint64_t size)
{
    CHECK_FAIL_RETURN_STATUS(!engines_.empty(), StatusCode::K_RUNTIME_ERROR, "No HCCS engines available");

    DsHixlEngineHandle engine = engines_.begin()->second;
    DsHixlMemHandle handle = nullptr;
    uint32_t vendorStatus = 0;
    DsHixlRegisterMemoryRequest request{ addr, size, DS_HIXL_MEMORY_DEVICE };
    DsHixlResult result = api_->register_memory(engine, &request, &handle, &vendorStatus);
    RETURN_IF_NOT_OK(HixlResultToStatus(result, vendorStatus, "register_memory(MEM_DEVICE)"));
    registeredDeviceMemories_.push_back(RegisteredDeviceMemory{ addr, size, handle });
    return Status::OK();
}

Status HixlTransport::ReleaseDeviceMemoryLocked(uintptr_t addr)
{
    auto registered = std::find_if(registeredDeviceMemories_.begin(), registeredDeviceMemories_.end(),
                                   [addr](const RegisteredDeviceMemory &memory) { return memory.addr == addr; });
    RETURN_OK_IF_TRUE(registered == registeredDeviceMemories_.end());

    auto engineIter = engines_.begin();
    CHECK_FAIL_RETURN_STATUS(engineIter != engines_.end(), StatusCode::K_RUNTIME_ERROR, "No HCCS engines available");
    uint32_t vendorStatus = 0;
    DsHixlResult result = api_->deregister_memory(engineIter->second, registered->handle, &vendorStatus);
    RETURN_IF_NOT_OK(HixlResultToStatus(result, vendorStatus, "deregister_memory(MEM_DEVICE)"));
    registeredDeviceMemories_.erase(registered);
    return Status::OK();
}

Status HixlTransport::ValidateDeviceMemoryRegistrationInputs(const std::vector<void *> &addrs,
                                                             const std::vector<uint64_t> &sizes) const
{
    CHECK_FAIL_RETURN_STATUS(!addrs.empty(), StatusCode::K_INVALID, "Device memory address list cannot be empty.");
    CHECK_FAIL_RETURN_STATUS(
        addrs.size() == sizes.size(), StatusCode::K_INVALID,
        FormatString("Device memory address count %zu does not match size count %zu.", addrs.size(), sizes.size()));
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HixlTransport not initialized");
    for (size_t i = 0; i < addrs.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(addrs[i] != nullptr, StatusCode::K_INVALID,
                                 FormatString("Device memory address cannot be null, index: %zu.", i));
        CHECK_FAIL_RETURN_STATUS(sizes[i] > 0, StatusCode::K_INVALID,
                                 FormatString("Device memory size must be greater than 0, index: %zu.", i));
    }
    return Status::OK();
}

std::string HixlTransport::FormatDeviceMemoryRanges(const std::vector<RegisteredDeviceMemory> &registrations)
{
    std::ostringstream ranges;
    ranges << "[";
    for (size_t i = 0; i < registrations.size(); ++i) {
        const auto &registration = registrations[i];
        if (i > 0) {
            ranges << ",";
        }
        ranges << "{addr:0x" << std::hex << registration.addr << std::dec << ",size:" << registration.size << "}";
    }
    ranges << "]";
    return ranges.str();
}

Status HixlTransport::PreRegisterDeviceMemory(const std::vector<void *> &addrs, const std::vector<uint64_t> &sizes)
{
    RETURN_IF_NOT_OK(ValidateDeviceMemoryRegistrationInputs(addrs, sizes));

    std::vector<RegisteredDeviceMemory> newRegistrations;
    std::unique_lock<std::mutex> lock(transferMutex_);
    // Build the exact set of ranges that will create new long-lived HIXL registrations first.
    // This keeps duplicate/covered inputs idempotent and lets us fail before partially registering
    // when the total MEM_DEVICE registration budget would be exceeded.
    for (size_t i = 0; i < addrs.size(); ++i) {
        uintptr_t addr = reinterpret_cast<uintptr_t>(addrs[i]);
        bool alreadyRegistered = HasRegisteredDeviceMemoryLocked(addr, sizes[i]);
        bool planned = std::any_of(newRegistrations.begin(), newRegistrations.end(),
                                   [addr, size = sizes[i]](const RegisteredDeviceMemory &memory) {
                                       return memory.addr <= addr && size <= memory.size
                                              && addr - memory.addr <= memory.size - size;
                                    });
        if (alreadyRegistered || planned) {
            continue;
        }
        size_t totalRegistrations = registeredDeviceMemories_.size() + newRegistrations.size() + 1;
        CHECK_FAIL_RETURN_STATUS(totalRegistrations <= MAX_DEVICE_REGISTRATIONS,
                                 StatusCode::K_RUNTIME_ERROR,
                                 FormatString("HCCS MEM_DEVICE pre-registration count %zu exceeds limit %zu.",
                                              totalRegistrations, MAX_DEVICE_REGISTRATIONS));
        newRegistrations.push_back(RegisteredDeviceMemory{ addr, sizes[i], nullptr });
    }

    std::vector<uintptr_t> retainedAddrs;
    retainedAddrs.reserve(newRegistrations.size());
    for (const auto &registration : newRegistrations) {
        Status status = RegisterDeviceMemoryLocked(registration.addr, registration.size);
        if (status.IsError()) {
            for (auto addr : retainedAddrs) {
                LOG_IF_ERROR(ReleaseDeviceMemoryLocked(addr), "Rollback pre-registered HCCS device memory failed");
            }
            return status;
        }
        retainedAddrs.emplace_back(registration.addr);
    }
    size_t totalRegistered = registeredDeviceMemories_.size();
    lock.unlock();

    LOG(INFO) << "[HCCS] PreRegisterDeviceMemory done, requested=" << addrs.size()
              << ", newlyRegistered=" << newRegistrations.size()
              << ", alreadyCovered=" << addrs.size() - newRegistrations.size()
              << ", totalRegistered=" << totalRegistered
              << ", ranges=" << FormatDeviceMemoryRanges(newRegistrations);
    return Status::OK();
}

Status HixlTransport::UnregisterDeviceMemory(const std::vector<void *> &addrs)
{
    RETURN_OK_IF_TRUE(addrs.empty());
    std::lock_guard<std::mutex> lock(transferMutex_);
    Status firstError = Status::OK();
    for (auto addr : addrs) {
        Status st = ReleaseDeviceMemoryLocked(reinterpret_cast<uintptr_t>(addr));
        if (st.IsError() && firstError.IsOk()) {
            firstError = st;
        }
    }
    return firstError;
}

void HixlTransport::ClearRegisteredDeviceMemory()
{
    std::lock_guard<std::mutex> lock(transferMutex_);
    if (registeredDeviceMemories_.empty()) {
        return;
    }

    auto engineIter = engines_.begin();
    if (engineIter == engines_.end()) {
        registeredDeviceMemories_.clear();
        return;
    }

    DsHixlEngineHandle engine = engineIter->second;
    for (auto &registered : registeredDeviceMemories_) {
        uint32_t vendorStatus = 0;
        DsHixlResult result = api_->deregister_memory(engine, registered.handle, &vendorStatus);
        LOG_IF(WARNING, result != DS_HIXL_OK)
            << "Failed to deregister HIXL memory handle, result=" << result << ", vendor_status=" << vendorStatus;
    }
    registeredDeviceMemories_.clear();
}

void HixlTransport::ClearRegisteredHostMemory()
{
    std::lock_guard<std::mutex> lock(transferMutex_);
    if (registeredHostMemories_.empty()) {
        return;
    }

    for (auto &registered : registeredHostMemories_) {
        auto engineIter = engines_.find(registered.devId);
        if (engineIter == engines_.end()) {
            LOG(WARNING) << "Failed to find HIXL engine when deregistering MEM_HOST on devId " << registered.devId;
            continue;
        }
        uint32_t vendorStatus = 0;
        DsHixlResult result = api_->deregister_memory(engineIter->second, registered.handle, &vendorStatus);
        LOG_IF(WARNING, result != DS_HIXL_OK) << "Failed to deregister HIXL host memory handle, result=" << result
                                              << ", vendor_status=" << vendorStatus;
    }
    registeredHostMemories_.clear();
}

Status HixlTransport::ImportRemoteAddressInfo(const std::string &remoteEndpoint, const RemoteHostSegmentPb &seg)
{
    (void)remoteEndpoint;
    (void)seg;
    // HCCS does not use remote memory info; addresses are carried in P2pScatterEntry::ddrBuf
    return Status::OK();
}

struct Batch {
    const DsHixlApi *api = nullptr;
    DsHixlEngineHandle engine = nullptr;
    std::string remoteEndpoint;
    size_t tempRegisterBudget = 0;
    size_t preRegisteredCount = 0;
    std::vector<DsHixlTransferDesc> descs;
    // These handles are temporary fallback registrations for the current batch only.
    std::vector<DsHixlMemHandle> handles;

    Batch(const DsHixlApi *hixlApi, DsHixlEngineHandle hixlEngine, const std::string &endpoint, size_t tempBudget,
          size_t preRegistered)
        : api(hixlApi),
          engine(hixlEngine),
          remoteEndpoint(endpoint),
          tempRegisterBudget(tempBudget),
          preRegisteredCount(preRegistered)
    {
    }

    void Reset()
    {
        if (api != nullptr && engine != nullptr) {
            for (auto handle : handles) {
                uint32_t vendorStatus = 0;
                DsHixlResult result = api->deregister_memory(engine, handle, &vendorStatus);
                LOG_IF(WARNING, result != DS_HIXL_OK)
                    << "Failed to deregister temporary HIXL memory handle, result=" << result
                    << ", vendor_status=" << vendorStatus;
            }
        }
        handles.clear();
        descs.clear();
    }

    ~Batch()
    {
        Reset();
    }
};

static Status FlushHixlBatch(Batch &batch)
{
    RETURN_OK_IF_TRUE(batch.descs.empty());
    CHECK_FAIL_RETURN_STATUS(batch.descs.size() <= std::numeric_limits<uint32_t>::max(), K_INVALID,
                             "HIXL transfer descriptor count exceeds uint32 range");
    uint32_t vendorStatus = 0;
    DsHixlTransferRequest request{ DsHixlStringView{ batch.remoteEndpoint.data(), batch.remoteEndpoint.size() },
                                   DS_HIXL_TRANSFER_READ,
                                   batch.descs.data(),
                                   static_cast<uint32_t>(batch.descs.size()),
                                   K_HIXL_TRANSFER_TIMEOUT_MS };
    DsHixlResult result = batch.api->transfer_sync(batch.engine, &request, &vendorStatus);
    RETURN_IF_NOT_OK(HixlResultToStatus(result, vendorStatus, "transfer_sync"));
    batch.Reset();
    return Status::OK();
}

static Status RegisterTemporaryDeviceMemoryForBatch(Batch &batch, uintptr_t localAddr, uint64_t len)
{
    CHECK_FAIL_RETURN_STATUS(batch.tempRegisterBudget > 0, StatusCode::K_RUNTIME_ERROR,
                             FormatString("No HCCS MEM_DEVICE registration budget left for fallback registration; "
                                          "pre-registered count: %zu, limit: %zu.",
                                          batch.preRegisteredCount, MAX_DEVICE_REGISTRATIONS));
    if (batch.handles.size() >= batch.tempRegisterBudget) {
        // Releasing the previous batch's temporary handles restores the remaining registration budget.
        RETURN_IF_NOT_OK(FlushHixlBatch(batch));
    }

    DsHixlMemHandle handle = nullptr;
    uint32_t vendorStatus = 0;
    DsHixlRegisterMemoryRequest request{ localAddr, len, DS_HIXL_MEMORY_DEVICE };
    DsHixlResult result = batch.api->register_memory(batch.engine, &request, &handle, &vendorStatus);
    RETURN_IF_NOT_OK(HixlResultToStatus(result, vendorStatus, "register_memory(MEM_DEVICE)"));
    batch.handles.push_back(handle);
    return Status::OK();
}

Status HixlTransport::ScatterBatch(P2pScatterEntry *entries, uint32_t count, const std::string &remoteEndpoint,
                                   std::shared_ptr<aclrtStream> stream)
{
    (void)stream;
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HixlTransport not initialized");

    std::lock_guard<std::mutex> lock(transferMutex_);

    CHECK_FAIL_RETURN_STATUS(!engines_.empty(), StatusCode::K_RUNTIME_ERROR, "No HCCS engines available");
    DsHixlEngineHandle engine = engines_.begin()->second;

    CHECK_FAIL_RETURN_STATUS(registeredDeviceMemories_.size() <= MAX_DEVICE_REGISTRATIONS,
                             StatusCode::K_RUNTIME_ERROR,
                             FormatString("HCCS MEM_DEVICE pre-registration count %zu exceeds limit %zu.",
                                          registeredDeviceMemories_.size(), MAX_DEVICE_REGISTRATIONS));

    // Pre-count the flattened descriptor count so the active HIXL batch reserves its TransferOpDesc vector
    // once (no growth/relocation while appending). HIXL TransferOpDesc count has no upper bound: HIXL
    // internally splits the submission across the SQ queue depth, so there is no operator-facing cap and no
    // mid-loop descriptor-driven flush here. The only mid-loop flush is temporary registration budget
    // exhaustion inside RegisterTemporaryDeviceMemoryForBatch.
    size_t totalDescriptorCount = 0;
    for (uint32_t i = 0; i < count; ++i) {
        const auto numEl = static_cast<size_t>(entries[i].numEl);
        CHECK_FAIL_RETURN_STATUS(totalDescriptorCount <= SIZE_MAX - numEl, K_INVALID,
                                 "Total HCCS descriptor count overflows size_t");
        totalDescriptorCount += numEl;
    }
    const size_t reserveCount = totalDescriptorCount;

    // Long-lived pre-registered handles and short-lived fallback handles share the HIXL registration budget.
    const size_t preRegisteredCount = registeredDeviceMemories_.size();
    const size_t tempRegisterBudget = MAX_DEVICE_REGISTRATIONS - preRegisteredCount;
    Batch batch(api_, engine, remoteEndpoint, tempRegisterBudget, preRegisteredCount);
    // Reserve the flattened TransferOpDesc count once so the descriptor vector does not relocate while appending.
    batch.descs.reserve(reserveCount);

    for (uint32_t i = 0; i < count; ++i) {
        const auto &entry = entries[i];
        uint64_t entryOffset = 0;
        uintptr_t remoteBase = reinterpret_cast<uintptr_t>(entry.ddrBuf);
        for (uint32_t j = 0; j < entry.numEl; ++j) {
            uintptr_t localAddr = reinterpret_cast<uintptr_t>(entry.dstBufs[j]);
            uintptr_t remoteAddr = remoteBase + entryOffset;
            uint64_t len = entry.counts[j];

            bool registered = HasRegisteredDeviceMemoryLocked(localAddr, len);
            if (!registered) {
                RETURN_IF_NOT_OK(RegisterTemporaryDeviceMemoryForBatch(batch, localAddr, len));
            }
            batch.descs.push_back(DsHixlTransferDesc{ localAddr, remoteAddr, len });
            entryOffset += entry.counts[j];
        }
    }

    return FlushHixlBatch(batch);
}

P2pLink HixlTransport::LinkType() const
{
    return P2P_LINK_HCCS;
}

}  // namespace datasystem
