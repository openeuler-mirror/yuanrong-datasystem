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

// HCCL registration limit: hold at most 256 MEM_DEVICE registrations
// at any one time. Register, transfer, and deregister each batch
// before starting the next.
constexpr uint32_t MAX_REGS_PER_BATCH = 253;

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

void HCCSTransport::SetLocalEndpoint(const std::string &ep, bool isClient)
{
    localIp_ = ep;
    isClient_ = isClient;
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

    const std::string &bufferPool = FLAGS_remote_h2d_hccs_buffer_pool;
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
              << " with buffer pool config: " << bufferPool;
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

    // Finalize all engines
    for (auto &[devId, engine] : engines_) {
        engine->Finalize();
    }
    engines_.clear();
    localEndpointById_.clear();
    initialized_ = false;
    return Status::OK();
}

Status HCCSTransport::RegisterMemory(void *addr, uint64_t size, P2pSegmentInfo *segInfo)
{
    // HCCS buffer-pool RH2D intentionally does not register the remote source host buffer. The source address
    // is carried inline by P2pScatterEntry::ddrBuf at transfer time; HIXL routes it through its internal
    // buffer-pool relay.
    (void)addr;
    (void)size;
    if (segInfo != nullptr) {
        int ret = memset_s(segInfo, sizeof(*segInfo), 0, sizeof(*segInfo));
        CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR, "Failed to clear HCCS segment info");
    }
    return Status::OK();
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
    std::vector<::hixl::TransferOpDesc> descs;
    std::vector<::hixl::MemHandle> handles;

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

Status HCCSTransport::ScatterBatch(P2pScatterEntry *entries, uint32_t count, const std::string &remoteEndpoint,
                                   std::shared_ptr<aclrtStream> stream)
{
    (void)stream;
    CHECK_FAIL_RETURN_STATUS(initialized_, StatusCode::K_RUNTIME_ERROR, "HCCSTransport not initialized");

    std::lock_guard<std::mutex> lock(transferMutex_);

    auto engine = engines_.begin()->second.get();

    Batch batch;
    batch.engine = engine;

    auto flushBatch = [&](Batch &b) -> Status {
        RETURN_OK_IF_TRUE(b.descs.empty());
        ::hixl::Status ret =
            engine->TransferSync(::hixl::AscendString(remoteEndpoint.c_str()), ::hixl::READ, b.descs, 10000);
        CHECK_FAIL_RETURN_STATUS(ret == ::hixl::SUCCESS, K_RUNTIME_ERROR,
                                 "Hixl::TransferSync failed: " + std::to_string(ret));
        b.Reset();
        return Status::OK();
    };

    for (uint32_t i = 0; i < count; ++i) {
        const auto &entry = entries[i];
        uint64_t entryOffset = 0;
        uintptr_t remoteBase = reinterpret_cast<uintptr_t>(entry.ddrBuf);
        for (uint32_t j = 0; j < entry.numEl; ++j) {
            uintptr_t localAddr = reinterpret_cast<uintptr_t>(entry.dstBufs[j]);
            uintptr_t remoteAddr = remoteBase + entryOffset;
            uint64_t len = entry.counts[j];

            // Register this local destination buffer.
            ::hixl::MemHandle handle = nullptr;
            ::hixl::MemDesc memDesc{ localAddr, len };
            ::hixl::Status rc = engine->RegisterMem(memDesc, ::hixl::MEM_DEVICE, handle);
            CHECK_FAIL_RETURN_STATUS(rc == ::hixl::SUCCESS, StatusCode::K_RUNTIME_ERROR,
                                     "Hixl::RegisterMem(MEM_DEVICE) failed: " + std::to_string(rc));
            batch.descs.push_back(::hixl::TransferOpDesc{ localAddr, remoteAddr, len });
            batch.handles.push_back(handle);
            entryOffset += entry.counts[j];

            if (batch.descs.size() >= MAX_REGS_PER_BATCH) {
                RETURN_IF_NOT_OK(flushBatch(batch));
            }
        }
    }

    return flushBatch(batch);
}

P2pLink HCCSTransport::LinkType() const
{
    return P2P_LINK_HCCS;
}

}  // namespace datasystem
