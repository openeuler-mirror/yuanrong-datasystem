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

/** Description: Defines the client transport facade. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H
#define DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H

#include <memory>

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/transport_advisor.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {
class TransportLayer {
public:
    explicit TransportLayer(std::shared_ptr<ClientRequestAuth> auth,
                            uint64_t fastTransportMemSize = DataPlaneManager::DEFAULT_FAST_TRANSPORT_MEM_SIZE,
                            BrpcChannelConfig channelConfig = {});
    ~TransportLayer();

    /** @brief Initialize transport runtime resources before data-plane connections are created. */
    Status Init();

    /**
     * @brief Execute Get and rebuild the selected data-plane connection once when required.
     * @param[in] workerAddr Address returned by the routing layer.
     * @param[in] input Logical Get request.
     * @param[out] output Get batches and their payload ownership.
     * @return K_OK on success; the error code otherwise.
     */
    Status Get(const HostPort &workerAddr, const TransportGetRequest &input, TransportGetResult &output);

    void Shutdown();

protected:
    /** @brief Construct the facade with injected collaborators for focused orchestration tests. */
    TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                   std::shared_ptr<TransportAdvisor> advisor);

private:
    std::shared_ptr<DataPlaneManager> dm_;
    std::shared_ptr<TransportAdvisor> advisor_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H
