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

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/object_read/object_read_flow.h"
#include "datasystem/client/transport/object_read/object_read_types.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class TransportLayer {
public:
    explicit TransportLayer(std::shared_ptr<Signature> signature, std::shared_ptr<ThreadPool> taskPool,
                            uint64_t fastTransportMemSize,
                            BrpcChannelConfig channelConfig = {});
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

    void Shutdown();

private:
    std::shared_ptr<DataPlaneManager> manager_;
    std::unique_ptr<ObjectReadFlow> objectRead_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_LAYER_H
