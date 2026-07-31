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

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

protected:
    // Separated from Connect so connection coordination can be tested without invoking the NPU runtime.
    virtual Status InitializeP2PComm(const std::string &remoteIdentity, P2pKind kind,
                                     std::function<int()> *heartbeatCallback, P2PComm &p2pComm, int32_t &devId);

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

    struct ConnectionInitState {
        std::mutex mutex;
        std::condition_variable cv;
        bool done = false;
        Status status;
    };

    Status WaitForConnectionInit(const std::shared_ptr<ConnectionInitState> &initState);
    void PublishConnectionInitResult(const std::string &remoteIdentity,
                                     const std::shared_ptr<ConnectionInitState> &initState, const Status &status);
    Status GetP2PCommContext(const std::string &remoteEndpoint, std::shared_ptr<P2PCommContext> &ctx);
    Status SubmitScatterBatch(P2pScatterEntry *entries, P2PComm p2pComm, aclrtStream stream, uint32_t start,
                              uint32_t size, uint32_t blobCount, bool isFinal);

    // connMutex_ only protects these maps and disconnect gates. The blocking NPU/TCP handshake is deliberately outside
    // this mutex. connectionInits_ provides single-flight initialization per remote identity, so duplicate calls share
    // one result while unrelated identities initialize concurrently.
    std::unordered_map<std::string, std::shared_ptr<P2PCommContext>> endpointToComm_;
    std::unordered_map<std::string, std::shared_ptr<ConnectionInitState>> connectionInits_;
    std::unordered_set<std::string> disconnectingEndpoints_;
    std::mutex connMutex_;
    std::condition_variable connCv_;
    bool disconnectAllInProgress_ = false;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_NPU_ROCE_TRANSPORT_H
