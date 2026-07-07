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

#ifndef DATASYSTEM_COMMON_RDMA_NPU_HCCS_TRANSPORT_H
#define DATASYSTEM_COMMON_RDMA_NPU_HCCS_TRANSPORT_H

#include "rh2d_transport_strategy.h"

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>
#include <atomic>

#include <hixl/hixl.h>
#include <hixl/hixl_types.h>

namespace datasystem {

enum class HixlMemoryMode { BUFFER_POOL, ROCE_DIRECT, FABRIC_MEM };

class HCCSTransport : public RH2DTransportStrategy {
public:
    HCCSTransport() = default;
    ~HCCSTransport() override;

    Status Init(const std::vector<int32_t> &deviceIds) override;
    Status GetConnectionIdentity(std::string *identity) override;
    Status Connect(const std::string &remoteIdentity, P2pKind kind, std::function<int()> *heartbeatCallback) override;
    Status Disconnect(const std::string &remoteIdentity) override;
    Status DisconnectAll() override;
    Status RegisterMemory(void *addr, uint64_t size, P2pSegmentInfo *segInfo) override;
    Status PreRegisterDeviceMemory(const std::vector<void *> &addrs, const std::vector<uint64_t> &sizes) override;
    Status UnregisterDeviceMemory(const std::vector<void *> &addrs) override;
    Status ImportRemoteAddressInfo(const std::string &remoteEndpoint, const RemoteHostSegmentPb &seg) override;
    Status ScatterBatch(P2pScatterEntry *entries, uint32_t count, const std::string &remoteEndpoint,
                        std::shared_ptr<aclrtStream> stream) override;
    P2pLink LinkType() const override;

    // Setter for HCCS local IP. Must be called before Init().
    void SetLocalEndpoint(const std::string &ep, bool isClient = false);

private:
    struct RegisteredDeviceMemory {
        uintptr_t addr;
        uint64_t size;
        ::hixl::MemHandle handle;
    };

    struct RegisteredHostMemory {
        int32_t devId;
        uintptr_t addr;
        uint64_t size;
        ::hixl::MemHandle handle;
    };

    Status InitializeSingleDevice(int32_t devId, const std::string &bufferPool);
    Status RegisterDeviceMemoryLocked(uintptr_t addr, uint64_t size);
    Status ReleaseDeviceMemoryLocked(uintptr_t addr);
    bool HasRegisteredDeviceMemoryLocked(uintptr_t addr, uint64_t size) const;
    bool HasRegisteredHostMemoryLocked(int32_t devId, uintptr_t addr, uint64_t size) const;
    void ClearRegisteredDeviceMemory();
    void ClearRegisteredHostMemory();
    bool IsHixlRoceDirectMode() const;

    // Per-device HIXL engines: devId -> engine
    std::map<int32_t, std::unique_ptr<::hixl::Hixl>> engines_;
    // Base local IP (shared across all devices)
    std::string localIp_;
    // Whether this process is a client
    bool isClient_ = false;
    // Per-device "ip:port" endpoint, derived from localIp_ + dynamic port
    std::map<int32_t, std::string> localEndpointById_;
    // Next engine index for round-robin identity selection
    std::atomic<unsigned int> nextEngineIndex_{ 0 };
    bool initialized_ = false;
    HixlMemoryMode hixlMemoryMode_ = HixlMemoryMode::BUFFER_POOL;
    std::unordered_set<std::string> activeEndpoints_;
    std::mutex connMutex_;
    // Serializes HIXL TransferSync and the lifetime of cached HIXL registrations.
    std::mutex transferMutex_;
    std::vector<RegisteredDeviceMemory> registeredDeviceMemories_;
    std::vector<RegisteredHostMemory> registeredHostMemories_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_NPU_HCCS_TRANSPORT_H
