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

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_RECEIVE_BUFFER_OWNER_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_RECEIVE_BUFFER_OWNER_H

#if defined(BUILD_PIPLN_H2D) && defined(USE_URMA)
#include <memory>

#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/common/rdma/urma_manager.h"

namespace datasystem {

// Keeps the client-direct RH2D receive buffer alive after MGetH2D returns.
//
// When the caller provides an external CUDA stream, MGetH2D only enqueues
// cudaMemcpyAsync and returns. The source host buffer must therefore stay valid
// until the caller synchronizes that stream. KVClient exposes the returned
// buffers through readOnlyBuffers; this owner is attached to the Buffer so the
// underlying receive and optional fallback UB BufferHandles are released only
// after the Buffer/ReadOnlyBuffer lifetime ends.
class DirectReceiveBufferOwner final : public client::IReceiveBufferOwner {
public:
    explicit DirectReceiveBufferOwner(std::shared_ptr<UrmaManager::BufferHandle> receiveHandle,
                                      std::shared_ptr<UrmaManager::BufferHandle> fallbackHandle = nullptr);

private:
    std::shared_ptr<UrmaManager::BufferHandle> receiveHandle_;
    std::shared_ptr<UrmaManager::BufferHandle> fallbackHandle_;
};

}  // namespace datasystem
#endif

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DIRECT_RECEIVE_BUFFER_OWNER_H
