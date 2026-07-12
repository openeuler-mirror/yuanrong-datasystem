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

/** Description: Defines the UB data-plane connection. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_UB_CONNECTION_H
#define DATASYSTEM_CLIENT_TRANSPORT_UB_CONNECTION_H

#include <atomic>
#include <memory>

#include "datasystem/client/transport/data_plane/i_data_plane_connection.h"
#include "datasystem/client/transport/rpc/worker_rpc_client.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

class UbConnection : public IDataPlaneConnection {
public:
    explicit UbConnection(std::shared_ptr<WorkerRpcClient> rpcClient = nullptr);
    ~UbConnection() override;

    Status Establish(const HostPort &workerAddr) override;
    bool IsAlive() const override;
    AccessTransportKind Kind() const override
    {
        return AccessTransportKind::UB;
    }
    void Teardown() override;

private:
    Status EstablishUrma(const HostPort &workerAddr);

    HostPort workerAddr_;
    std::shared_ptr<WorkerRpcClient> rpcClient_;
    std::atomic<bool> urmaReady_{ false };
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_UB_CONNECTION_H
