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

/** Description: Owns the ShmSession-backed receive buffer released to the data worker. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_SHM_RECEIVE_BUFFER_OWNER_H
#define DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_SHM_RECEIVE_BUFFER_OWNER_H

#include <atomic>
#include <memory>

#include "datasystem/client/mmap/immap_table_entry.h"
#include "datasystem/client/transport/data_plane/i_data_transporter.h"
#include "datasystem/client/transport/data_plane/shm_connection.h"
#include "datasystem/client/transport/object_read/object_read_types.h"
#include "datasystem/common/object_cache/ireceive_buffer_owner.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace client {

/** @brief Receive-buffer owner that releases the target worker reference through the ShmSession. */
class ShmReceiveBufferOwner final : public IReceiveBufferOwner {
public:
    ShmReceiveBufferOwner(std::shared_ptr<ShmSession> session, std::shared_ptr<IMmapTableEntry> mmapEntry,
                          ShmKey shmId, std::shared_ptr<const TransportReadContext> context,
                          std::weak_ptr<ThreadPool> releasePool);
    ~ShmReceiveBufferOwner() override;

    void Release() override;
    bool ManagesWorkerReference() const override;
    Status CheckAlive() const override;
    bool IsCudaHostMemoryRegistrationDone() const override;

private:
    std::shared_ptr<ShmSession> session_;
    std::shared_ptr<IMmapTableEntry> mmapEntry_;
    ShmKey shmId_;
    std::shared_ptr<const TransportReadContext> context_;
    std::weak_ptr<ThreadPool> releasePool_;
    std::atomic<bool> released_{ false };
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_SHM_RECEIVE_BUFFER_OWNER_H
