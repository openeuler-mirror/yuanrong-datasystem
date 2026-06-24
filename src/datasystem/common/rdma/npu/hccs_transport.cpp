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

#include "datasystem/common/rdma/npu/hccs_transport.h"

#include <algorithm>
#include <netdb.h>
#include <netinet/in.h>
#include <random>
#include <securec.h>
#include <sys/socket.h>
#include <unistd.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/common/util/status_helper.h"

DS_DECLARE_string(remote_h2d_hccs_buffer_pool);

namespace {

// Fixed device-NIC listen port for worker one-sided comm. Each worker device has its own
// device IP, so the same port across devices does not collide.
constexpr int K_WORKER_DEVICE_COMM_PORT = 26666;
constexpr int K_EPHEMERAL_PORT_START = 49152;
constexpr int K_EPHEMERAL_PORT_END = 65535;
constexpr int K_MAX_PORT_ALLOC_ATTEMPTS = 5;
const std::string K_HCCL_INTRA_ROCE_ENABLE = "HCCL_INTRA_ROCE_ENABLE";
const std::string K_HIXL_DIRECT_ROCE_BUFFER_POOL = "0:0";

// HCCL registration limit: keep a small reserve under the 256 MEM_DEVICE limit.
// Long-lived pre-registrations and temporary fallback registrations share this budget.
constexpr size_t MAX_DEVICE_REGISTRATIONS = 253;
constexpr size_t MAX_TRANSFER_DESCS_PER_BATCH = 1024;

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

}  // namespace

namespace datasystem {

HCCSTransport::~HCCSTransport()
{
    ClearRegisteredDeviceMemory();
    ClearRegisteredHostMemory();
}

void HCCSTransport::SetLocalEndpoint(const std::string &ep, bool isClient)
{
    localIp_ = ep;
    isClient_ = isClient;
}

bool HCCSTransport::IsHixlRoceDirectMode() const
{
    return hixlMemoryMode_ == HixlMemoryMode::ROCE_DIRECT;
}

Status HCCSTransport::InitializeSingleDevice(int32_t devId, const std::string &bufferPool)
{
    Status setDevSt = acl::AclDeviceManager::Instance()->SetDeviceIdx(devId);
    if (setDevSt.IsError()) {
        LOG(WARNING) << "[HCCS] SetDeviceIdx failed for devId " << devId << ": " << setDevSt.GetMsg()
                     << "; skipping this device";
        return setDevSt;
    }

    int port = AllocateRandomPort();
    if (port <= 0) {
        LOG(WARNING) << "[HCCS] Failed to allocate random port for devId " << devId << "; skipping this device";
        return Status(StatusCode::K_RUNTIME_ERROR, "Failed to allocate port for at least one device");
    }

    std::string endpoint = localIp_ + ":" + std::to_string(port);
    LOG(INFO) << "[HCCS] Hixl::Initialize start devId=" << devId << " endpoint=" << endpoint;
    auto engine = std::make_unique<::hixl::Hixl>();
    std::map<::hixl::AscendString, ::hixl::AscendString> options;
    options[::hixl::AscendString(::hixl::OPTION_BUFFER_POOL)] = ::hixl::AscendString(bufferPool.c_str());
    if (!isClient_) {
        std::string resourceConfig =
            R"({"comm_resource_config.listen_port":")" + std::to_string(K_WORKER_DEVICE_COMM_PORT) + R"("})";
        options[::hixl::AscendString(::hixl::OPTION_GLOBAL_RESOURCE_CONFIG)] =
            ::hixl::AscendString(resourceConfig.c_str());
    }

    ::hixl::Status ret = engine->Initialize(::hixl::AscendString(endpoint.c_str()), options);
    if (ret != ::hixl::SUCCESS) {
        LOG(WARNING) << "[HCCS] Hixl::Initialize failed on devId " << devId << " endpoint " << endpoint << ": " << ret
                     << "; skipping this device";
        return Status(StatusCode::K_RUNTIME_ERROR, "Hixl::Initialize failed for at least one device");
    }

    engines_[devId] = std::move(engine);
    localEndpointById_[devId] = endpoint;
    LOG(INFO) << "[HCCS] Engine created for devId " << devId << " at " << endpoint;
    return Status::OK();
}

Status HCCSTransport::Init(const std::vector<int32_t> &deviceIds)
{
    LOG(INFO) << "[HCCS] Init localIp=" << localIp_ << " numDevices=" << deviceIds.size();
    RETURN_OK_IF_TRUE(initialized_);
    CHECK_FAIL_RETURN_STATUS(!localIp_.empty(), StatusCode::K_INVALID,
                             "HCCS local IP not configured. Call SetLocalEndpoint()");

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
    Status firstError = Status::OK();
    for (int32_t devId : targetDevIds) {
        Status st = InitializeSingleDevice(devId, bufferPool);
        if (st.IsError()) {
            if (firstError.IsOk()) {
                firstError = st;
            }
            continue;
        }
    }

    if (engines_.empty()) {
        return Status(StatusCode::K_RUNTIME_ERROR, "No HCCS engines could be initialized");
    }

    initialized_ = true;
    LOG(INFO) << "[HCCS] Initialized with " << engines_.size() << " engine(s) on IP " << localIp_
              << " with hixl memory mode: " << HixlMemoryModeName(hixlMemoryMode_)
              << ", buffer pool config: " << bufferPool;
    return Status::OK();
}

Status HCCSTransport::GetConnectionIdentity(std::string *identity)
{
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HCCSTransport not initialized");
    CHECK_FAIL_RETURN_STATUS(!engines_.empty(), StatusCode::K_RUNTIME_ERROR, "No HCCS engines available");

    // Round-robin across engines so each new p2p connection is distributed
    // across different device IDs and, therefore, different NPU cards.
    unsigned int idx = nextEngineIndex_.fetch_add(1, std::memory_order_relaxed);
    auto it = engines_.begin();
    std::advance(it, idx % engines_.size());
    *identity = localEndpointById_[it->first];
    return Status::OK();
}

Status HCCSTransport::Connect(const std::string &remoteIdentity, P2pKind kind, std::function<int()> *heartbeatCallback)
{
    (void)heartbeatCallback;
    LOG(INFO) << "[HCCS] Connect enter, remote=" << remoteIdentity
              << " kind=" << (kind == P2P_SENDER ? "SENDER" : "RECEIVER");
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HCCSTransport not initialized");

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
    ::hixl::Hixl *engine = engines_.begin()->second.get();

    {
        std::lock_guard<std::mutex> lock(connMutex_);
        if (activeEndpoints_.find(remoteIdentity) != activeEndpoints_.end()) {
            LOG(INFO) << "[HCCS] Already connected to " << remoteIdentity << ", skipping";
            return Status::OK();
        }

        // One HIXL engine can manage connections to multiple remote endpoints.
        LOG(INFO) << "[HCCS] Connecting from local endpoint to " << remoteIdentity;

        ::hixl::Status rc = engine->Connect(::hixl::AscendString(remoteIdentity.c_str()), 30000);
        if (rc != ::hixl::SUCCESS && rc != ::hixl::ALREADY_CONNECTED) {
            LOG(ERROR) << "[HCCS] Hixl::Connect failed to " << remoteIdentity << ", status=" << rc;
            return Status(StatusCode::K_RUNTIME_ERROR,
                          "Hixl::Connect failed: " + std::to_string(rc) + ", remote endpoint: " + remoteIdentity);
        }

        activeEndpoints_.insert(remoteIdentity);
        LOG(INFO) << "[HCCS] Connected to " << remoteIdentity;
    }
    return Status::OK();
}

Status HCCSTransport::Disconnect(const std::string &remoteIdentity)
{
    std::lock_guard<std::mutex> lock(connMutex_);
    RETURN_OK_IF_TRUE(activeEndpoints_.erase(remoteIdentity) == 0);

    // Use the first engine, consistent with Connect and ScatterBatch
    auto eng = engines_.begin();
    if (eng != engines_.end()) {
        ::hixl::Status ret = eng->second->Disconnect(::hixl::AscendString(remoteIdentity.c_str()), 1000);
        CHECK_FAIL_RETURN_STATUS(ret == ::hixl::SUCCESS, StatusCode::K_RUNTIME_ERROR,
                                 "Hixl::Disconnect from " + remoteIdentity + " failed: " + std::to_string(ret));
        LOG(INFO) << "[HCCS] Disconnected from " + remoteIdentity;
    }

    return Status::OK();
}

Status HCCSTransport::DisconnectAll()
{
    if (!initialized_)
        return Status::OK();

    // Use the first engine, consistent with Connect and ScatterBatch
    auto eng = engines_.begin();
    if (eng != engines_.end()) {
        std::lock_guard<std::mutex> lock(connMutex_);
        for (const auto &remoteId : activeEndpoints_) {
            ::hixl::Status ret = eng->second->Disconnect(::hixl::AscendString(remoteId.c_str()), 1000);
            if (ret != ::hixl::SUCCESS) {
                LOG(WARNING) << "[HCCS] Failed to disconnect from " << remoteId << ": " << ret;
            } else {
                LOG(INFO) << "[HCCS] Disconnected from " + remoteId;
            }
        }
        activeEndpoints_.clear();
    }

    ClearRegisteredDeviceMemory();
    ClearRegisteredHostMemory();

    // Finalize all engines
    for (auto &[devId, engine] : engines_) {
        engine->Finalize();
    }
    engines_.clear();
    localEndpointById_.clear();
    hixlMemoryMode_ = HixlMemoryMode::BUFFER_POOL;
    initialized_ = false;
    return Status::OK();
}

Status HCCSTransport::RegisterMemory(void *addr, uint64_t size, P2pSegmentInfo *segInfo)
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
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HCCSTransport not initialized");
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

    ::hixl::MemDesc memDesc{ hostAddr, size };
    ::hixl::MemHandle handle = nullptr;
    ::hixl::Status rc = engineIter->second->RegisterMem(memDesc, ::hixl::MEM_HOST, handle);
    CHECK_FAIL_RETURN_STATUS(rc == ::hixl::SUCCESS, StatusCode::K_RUNTIME_ERROR,
                             "Hixl::RegisterMem(MEM_HOST) failed: " + std::to_string(rc));
    registeredHostMemories_.push_back(RegisteredHostMemory{ devId, hostAddr, size, handle });
    return Status::OK();
}

bool HCCSTransport::HasRegisteredHostMemoryLocked(int32_t devId, uintptr_t addr, uint64_t size) const
{
    return std::any_of(registeredHostMemories_.begin(), registeredHostMemories_.end(),
                       [devId, addr, size](const RegisteredHostMemory &registered) {
                           return registered.devId == devId && registered.addr <= addr && size <= registered.size
                                  && addr - registered.addr <= registered.size - size;
                        });
}

bool HCCSTransport::HasRegisteredDeviceMemoryLocked(uintptr_t addr, uint64_t size) const
{
    return std::any_of(registeredDeviceMemories_.begin(), registeredDeviceMemories_.end(),
                       [addr, size](const RegisteredDeviceMemory &registered) {
                           return registered.addr <= addr && size <= registered.size
                                  && addr - registered.addr <= registered.size - size;
                        });
}

Status HCCSTransport::RegisterDeviceMemoryLocked(uintptr_t addr, uint64_t size)
{
    CHECK_FAIL_RETURN_STATUS(!engines_.empty(), StatusCode::K_RUNTIME_ERROR, "No HCCS engines available");

    auto engine = engines_.begin()->second.get();
    ::hixl::MemHandle handle = nullptr;
    ::hixl::MemDesc memDesc{ addr, size };
    ::hixl::Status rc = engine->RegisterMem(memDesc, ::hixl::MEM_DEVICE, handle);
    CHECK_FAIL_RETURN_STATUS(rc == ::hixl::SUCCESS, StatusCode::K_RUNTIME_ERROR,
                             "Hixl::RegisterMem(MEM_DEVICE) failed: " + std::to_string(rc));
    registeredDeviceMemories_.push_back(RegisteredDeviceMemory{ addr, size, handle });
    return Status::OK();
}

Status HCCSTransport::ReleaseDeviceMemoryLocked(uintptr_t addr)
{
    auto registered = std::find_if(registeredDeviceMemories_.begin(), registeredDeviceMemories_.end(),
                                   [addr](const RegisteredDeviceMemory &memory) { return memory.addr == addr; });
    RETURN_OK_IF_TRUE(registered == registeredDeviceMemories_.end());

    auto engineIter = engines_.begin();
    CHECK_FAIL_RETURN_STATUS(engineIter != engines_.end(), StatusCode::K_RUNTIME_ERROR, "No HCCS engines available");
    ::hixl::Status rc = engineIter->second->DeregisterMem(registered->handle);
    CHECK_FAIL_RETURN_STATUS(rc == ::hixl::SUCCESS, StatusCode::K_RUNTIME_ERROR,
                             "Hixl::DeregisterMem(MEM_DEVICE) failed: " + std::to_string(rc));
    registeredDeviceMemories_.erase(registered);
    return Status::OK();
}

Status HCCSTransport::PreRegisterDeviceMemory(const std::vector<void *> &addrs, const std::vector<uint64_t> &sizes)
{
    CHECK_FAIL_RETURN_STATUS(!addrs.empty(), StatusCode::K_INVALID, "Device memory address list cannot be empty.");
    CHECK_FAIL_RETURN_STATUS(
        addrs.size() == sizes.size(), StatusCode::K_INVALID,
        FormatString("Device memory address count %zu does not match size count %zu.", addrs.size(), sizes.size()));
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HCCSTransport not initialized");
    for (size_t i = 0; i < addrs.size(); ++i) {
        CHECK_FAIL_RETURN_STATUS(addrs[i] != nullptr, StatusCode::K_INVALID,
                                 FormatString("Device memory address cannot be null, index: %zu.", i));
        CHECK_FAIL_RETURN_STATUS(sizes[i] > 0, StatusCode::K_INVALID,
                                 FormatString("Device memory size must be greater than 0, index: %zu.", i));
    }

    std::lock_guard<std::mutex> lock(transferMutex_);
    // Build the exact set of ranges that will create new long-lived HIXL registrations first.
    // This keeps duplicate/covered inputs idempotent and lets us fail before partially registering
    // when the total MEM_DEVICE registration budget would be exceeded.
    std::vector<RegisteredDeviceMemory> newRegistrations;
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
    return Status::OK();
}

Status HCCSTransport::UnregisterDeviceMemory(const std::vector<void *> &addrs)
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

void HCCSTransport::ClearRegisteredDeviceMemory()
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

    auto engine = engineIter->second.get();
    for (auto &registered : registeredDeviceMemories_) {
        ::hixl::Status rc = engine->DeregisterMem(registered.handle);
        LOG_IF(WARNING, rc != ::hixl::SUCCESS) << "Failed to deregister HIXL memory handle: " << rc;
    }
    registeredDeviceMemories_.clear();
}

void HCCSTransport::ClearRegisteredHostMemory()
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
        ::hixl::Status rc = engineIter->second->DeregisterMem(registered.handle);
        LOG_IF(WARNING, rc != ::hixl::SUCCESS) << "Failed to deregister HIXL host memory handle: " << rc;
    }
    registeredHostMemories_.clear();
}

Status HCCSTransport::ImportRemoteAddressInfo(const std::string &remoteEndpoint, const RemoteHostSegmentPb &seg)
{
    (void)remoteEndpoint;
    (void)seg;
    // HCCS does not use remote memory info; addresses are carried in P2pScatterEntry::ddrBuf
    return Status::OK();
}

struct Batch {
    ::hixl::Hixl *engine = nullptr;
    std::string remoteEndpoint;
    size_t tempRegisterBudget = 0;
    size_t preRegisteredCount = 0;
    std::vector<::hixl::TransferOpDesc> descs;
    // These handles are temporary fallback registrations for the current batch only.
    std::vector<::hixl::MemHandle> handles;

    Batch(::hixl::Hixl *hixlEngine, const std::string &endpoint, size_t tempBudget, size_t preRegistered)
        : engine(hixlEngine),
          remoteEndpoint(endpoint),
          tempRegisterBudget(tempBudget),
          preRegisteredCount(preRegistered)
    {
    }

    void Reset()
    {
        if (engine != nullptr) {
            for (auto handle : handles) {
                ::hixl::Status rc = engine->DeregisterMem(handle);
                LOG_IF(WARNING, rc != ::hixl::SUCCESS) << "Failed to deregister HIXL memory handle: " << rc;
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
    ::hixl::Status ret = batch.engine->TransferSync(::hixl::AscendString(batch.remoteEndpoint.c_str()), ::hixl::READ,
                                                    batch.descs, 10000);
    CHECK_FAIL_RETURN_STATUS(ret == ::hixl::SUCCESS, K_RUNTIME_ERROR,
                             "Hixl::TransferSync failed: " + std::to_string(ret));
    batch.Reset();
    return Status::OK();
}

static Status FlushHixlBatchIfDescFull(Batch &batch)
{
    RETURN_OK_IF_TRUE(batch.descs.size() < MAX_TRANSFER_DESCS_PER_BATCH);
    return FlushHixlBatch(batch);
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

    ::hixl::MemHandle handle = nullptr;
    ::hixl::MemDesc memDesc{ localAddr, len };
    ::hixl::Status rc = batch.engine->RegisterMem(memDesc, ::hixl::MEM_DEVICE, handle);
    CHECK_FAIL_RETURN_STATUS(rc == ::hixl::SUCCESS, StatusCode::K_RUNTIME_ERROR,
                             "Hixl::RegisterMem(MEM_DEVICE) failed: " + std::to_string(rc));
    batch.handles.push_back(handle);
    return Status::OK();
}

Status HCCSTransport::ScatterBatch(P2pScatterEntry *entries, uint32_t count, const std::string &remoteEndpoint,
                                   std::shared_ptr<aclrtStream> stream)
{
    (void)stream;
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HCCSTransport not initialized");

    std::lock_guard<std::mutex> lock(transferMutex_);

    auto engine = engines_.begin()->second.get();

    CHECK_FAIL_RETURN_STATUS(registeredDeviceMemories_.size() <= MAX_DEVICE_REGISTRATIONS,
                             StatusCode::K_RUNTIME_ERROR,
                             FormatString("HCCS MEM_DEVICE pre-registration count %zu exceeds limit %zu.",
                                          registeredDeviceMemories_.size(), MAX_DEVICE_REGISTRATIONS));
    // Long-lived pre-registered handles and short-lived fallback handles share the HIXL registration budget.
    const size_t preRegisteredCount = registeredDeviceMemories_.size();
    const size_t tempRegisterBudget = MAX_DEVICE_REGISTRATIONS - preRegisteredCount;
    Batch batch(engine, remoteEndpoint, tempRegisterBudget, preRegisteredCount);

    for (uint32_t i = 0; i < count; ++i) {
        const auto &entry = entries[i];
        uint64_t entryOffset = 0;
        uintptr_t remoteBase = reinterpret_cast<uintptr_t>(entry.ddrBuf);
        for (uint32_t j = 0; j < entry.numEl; ++j) {
            uintptr_t localAddr = reinterpret_cast<uintptr_t>(entry.dstBufs[j]);
            uintptr_t remoteAddr = remoteBase + entryOffset;
            uint64_t len = entry.counts[j];

            bool registered = HasRegisteredDeviceMemoryLocked(localAddr, len);
            RETURN_IF_NOT_OK(FlushHixlBatchIfDescFull(batch));
            if (!registered) {
                RETURN_IF_NOT_OK(RegisterTemporaryDeviceMemoryForBatch(batch, localAddr, len));
            }
            batch.descs.push_back(::hixl::TransferOpDesc{ localAddr, remoteAddr, len });
            entryOffset += entry.counts[j];
        }
    }

    return FlushHixlBatch(batch);
}

P2pLink HCCSTransport::LinkType() const
{
    return P2P_LINK_HCCS;
}

}  // namespace datasystem
