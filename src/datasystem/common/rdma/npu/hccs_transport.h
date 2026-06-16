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

class HCCSTransport : public RH2DTransportStrategy {
public:
    HCCSTransport() = default;
    ~HCCSTransport() override = default;

    Status Init(const std::vector<int32_t> &deviceIds) override;
    Status GetConnectionIdentity(std::string *identity) override;
    Status Connect(const std::string &remoteIdentity, P2pKind kind, std::function<int()> *heartbeatCallback) override;
    Status Disconnect(const std::string &remoteIdentity) override;
    Status DisconnectAll() override;
    Status RegisterMemory(void *addr, uint64_t size, P2pSegmentInfo *segInfo) override;
    Status ImportRemoteAddressInfo(const std::string &remoteEndpoint, const RemoteHostSegmentPb &seg) override;
    Status ScatterBatch(P2pScatterEntry *entries, uint32_t count, const std::string &remoteEndpoint,
                        std::shared_ptr<aclrtStream> stream) override;
    P2pLink LinkType() const override;

    // Setter for HCCS local IP. Must be called before Init().
    void SetLocalEndpoint(const std::string &ep, bool isClient = false);

private:
    Status InitializeSingleDevice(int32_t devId, const std::string &bufferPool);

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
    std::unordered_set<std::string> activeEndpoints_;
    std::mutex connMutex_;
    std::mutex transferMutex_;  // serialize TransferSync calls
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_NPU_HCCS_TRANSPORT_H
