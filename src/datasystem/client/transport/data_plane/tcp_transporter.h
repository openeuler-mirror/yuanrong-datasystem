/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/** Description: Defines the TCP object data transporter. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_TCP_TRANSPORTER_H
#define DATASYSTEM_CLIENT_TRANSPORT_TCP_TRANSPORTER_H

#include <memory>
#include <utility>

#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"

namespace datasystem {
namespace client {
class TcpTransporter : public IDataTransporter {
public:
    explicit TcpTransporter(std::shared_ptr<WorkerRpcClient> rpcClient) : rpcClient_(std::move(rpcClient))
    {
    }
    ~TcpTransporter() override = default;

    AccessTransportKind Kind() const override
    {
        return AccessTransportKind::TCP;
    }

    bool IsAlive() const override
    {
        return rpcClient_ != nullptr && rpcClient_->IsAlive();
    }

    Status Get(const DataGetRequest &input, DataGetResult &output) override;

    Status Create(const HostPort &workerAddr, const std::string &key, uint64_t size,
                  const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer) override;
    Status Set(ObjectBuffer &buffer, const TransportSetParam &param) override;
    Status MCreate(const HostPort &workerAddr, const std::vector<std::string> &keys,
                   const std::vector<uint64_t> &sizes, const TransportCreateParam &param,
                   std::vector<std::shared_ptr<ObjectBuffer>> &buffers) override;
    Status MSet(const std::vector<std::shared_ptr<ObjectBuffer>> &buffers,
                const TransportSetParam &param, TransportMSetResult &result) override;
    Status Release(const ShmKey &shmId, const TransportRequestContext &context) override;

private:
    Status CreateBuffer(const HostPort &workerAddr, const std::string &key, uint64_t size,
                        const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer);

    std::shared_ptr<WorkerRpcClient> rpcClient_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TCP_TRANSPORTER_H
