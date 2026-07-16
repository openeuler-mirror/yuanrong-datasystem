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

/**
 * Description: Per-(client,worker) shared-memory connection state for the multi-worker flow.
 * See shm-and-heartbeat design §4.5 ShmEntry.
 */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_SHM_ENTRY_H
#define DATASYSTEM_CLIENT_TRANSPORT_SHM_ENTRY_H

#include <cstdint>
#include <string>

#include "datasystem/client/transport/shm_fd.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

/**
 * @brief State of one (client, worker) shm connection. One ShmEntry lives per same-host worker in
 * ShmConnectionPool. The fd-passing socketFd is owned via ShmFd (RAII, move-only) so the entry is
 * move-only: copies cannot dangle a closed fd or double-close it (review fix #2). The data shmFd/
 * shmId/mmapRegion are filled lazily on the first data operation (Create/Get), per design §4.3.2,
 * and shmFd/mmapRegion lifetime is managed by MmapManager, not this entry.
 */
struct ShmEntry {
    HostPort workerAddr;            // worker address (key).
    ShmFd socketFd;                 // fd-passing channel fd (UDS/SCMTCP), RAII-owned, obtained at handshake.
    int shmFd = INVALID_SHM_FD;     // shm data fd, lazily obtained on first data op; owned by MmapManager.
    std::string shmId;              // worker-assigned unique shm region id (worker_uuid-prefixed).
    std::string workerStartId;     // worker process epoch (refreshed on reconnect).
    void *mmapRegion = nullptr;    // mmap'd region pointer; owned by MmapManager.
    uint64_t mmapSize = 0;         // mmap'd region size.

    ShmEntry() = default;
    ShmEntry(const ShmEntry &) = delete;
    ShmEntry &operator=(const ShmEntry &) = delete;
    ShmEntry(ShmEntry &&) = default;
    ShmEntry &operator=(ShmEntry &&) = default;

    /**
     * @brief A connection is valid once its fd-passing channel has been established. shmFd may still
     * be INVALID_SHM_FD (lazy) on a valid entry.
     */
    bool IsValid() const
    {
        return socketFd.IsValid();
    }
};
}  // namespace client
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_TRANSPORT_SHM_ENTRY_H
