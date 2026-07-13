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

#include <cstdint>
#include <memory>
#include <string>

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/object_read/object_read_flow.h"
#include "datasystem/client/transport/object_read/object_read_types.h"
#include "datasystem/client/transport/rpc/set_request_builder.h"
#include "datasystem/client/transport/transport_advisor.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/object/object_buffer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

class TransportLayer {
public:
    explicit TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                            uint64_t fastTransportMemSize, BrpcChannelConfig channelConfig = {});
    ~TransportLayer();

    /** @brief Initialize transport runtime resources before data-plane connections are created. */
    Status Init();

    /**
     * @brief Execute an object read through metadata lookup and direct data-worker access.
     * @param[in] input Routed object read request.
     * @param[out] output Owned object read results.
     * @return K_OK on success; the error code otherwise.
     */
    Status Get(const ObjectReadRequest &input, ObjectReadResult &output);

    /**
     * @brief Create an ObjectBuffer with transport-native memory.
     * @param[in] workerAddr Address returned by the routing layer.
     * @param[in] objectKey Object key.
     * @param[in] dataSize Data capacity in bytes.
     * @param[in] param Create parameters.
     * @param[out] buffer Created ObjectBuffer.
     * @return K_OK on success; the error code otherwise.
     */
    Status Create(const HostPort &workerAddr, const std::string &objectKey, uint64_t dataSize,
                  const TransportCreateParam &param, std::shared_ptr<ObjectBuffer> &buffer);

    /**
     * @brief Commit an ObjectBuffer through the selected transport.
     * @param[in] buffer ObjectBuffer created through Create.
     * @param[in] param Publish parameters.
     * @return K_OK on success; the error code otherwise.
     */
    Status Set(ObjectBuffer &buffer, const TransportSetParam &param);

    void Shutdown();

protected:
    /** @brief Construct the facade with injected collaborators for focused orchestration tests. */
    TransportLayer(std::shared_ptr<DataPlaneManager> dataPlaneManager,
                   std::shared_ptr<TransportAdvisor> advisor);

private:
    std::shared_ptr<DataPlaneManager> manager_;
    std::shared_ptr<TransportAdvisor> advisor_;
    std::unique_ptr<ObjectReadFlow> objectRead_;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H
