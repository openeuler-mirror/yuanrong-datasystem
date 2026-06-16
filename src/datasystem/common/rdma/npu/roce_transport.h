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

#ifndef DATASYSTEM_COMMON_RDMA_NPU_ROCE_TRANSPORT_H
#define DATASYSTEM_COMMON_RDMA_NPU_ROCE_TRANSPORT_H

#include "rh2d_transport_strategy.h"

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace datasystem {

class RoCETransport : public RH2DTransportStrategy {
public:
    RoCETransport() = default;
    ~RoCETransport() override = default;

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

private:
    struct P2PCommContext {
        P2PCommContext(P2PComm p2pComm, int32_t deviceId) : comm(p2pComm), devId(deviceId)
        {
        }
        P2PCommContext(const P2PCommContext &) = delete;
        P2PCommContext &operator=(const P2PCommContext &) = delete;
        ~P2PCommContext();

        P2PComm comm = nullptr;
        int32_t devId = -1;
    };

    // Map from remote endpoint (base64 rootInfo) to P2PComm handle and its device context.
    // Uses std::shared_ptr on P2PCommContext to manage connection lifetime:
    //   - ScatterBatch copies the shared_ptr, keeping the connection alive after releasing the lock.
    //   - Disconnect simply removes the map entry; the underlying P2PComm is destroyed when the last
    //     shared_ptr reference is released (via ~P2PCommContext()).
    std::unordered_map<std::string, std::shared_ptr<P2PCommContext>> endpointToComm_;
    std::mutex connMutex_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_NPU_ROCE_TRANSPORT_H
