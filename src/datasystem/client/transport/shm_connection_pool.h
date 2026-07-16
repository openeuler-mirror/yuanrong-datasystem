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
 * Description: Manages N same-host workers' independent shm connections.
 * Each same-host worker owns one ShmEntry (socketFd + lazily-filled shmFd/mmapRegion).
 * Keyed by worker address. See shm-and-heartbeat design §4.1/§4.3.2/§4.6.
 */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_SHM_CONNECTION_POOL_H
#define DATASYSTEM_CLIENT_TRANSPORT_SHM_CONNECTION_POOL_H

#include <functional>
#include <string>

#include <tbb/concurrent_hash_map.h>

#include "datasystem/client/transport/shm_entry.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

/**
 * @brief Builds a ShmEntry for a worker. In production this wraps the three handshake steps
 * (GetSocketPath -> UDS/SCMTCP HandShakeConnect -> RegisterClient), refactored out of
 * ClientWorkerRemoteCommonApi::Connect so the pool does not own a per-worker api instance
 * (see design §4.3.2 ShmConnectContext). For tests, a stub is injected.
 *
 * @param[in] workerAddr Target worker address.
 * @param[out] entry Move-constructed result (socketFd set; shmFd/shmId/mmapRegion lazily left to
 *                  MmapManager). The connector fills the entry's owning fields.
 * @return Status of the handshake.
 */
using ShmConnector = std::function<Status(const HostPort &workerAddr, ShmEntry &entry)>;

class ShmConnectionPool {
public:
    using EntryMap = tbb::concurrent_hash_map<std::string, ShmEntry>;

    /**
     * @brief Construct the pool. The connector builds a fresh ShmEntry for a worker on demand.
     * @param[in] sdkHostId The sdk's own host id (for same/cross-host decisions by the caller).
     * @param[in] connector Worker handshake function (injected so the pool is unit-testable).
     */
    ShmConnectionPool(std::string sdkHostId, ShmConnector connector);

    ~ShmConnectionPool();

    /**
     * @brief Get an existing entry or build a new one for the worker (UC5/UC9). The connector runs
     * OUTSIDE the per-key lock so concurrent callers for the same worker are not serialized on the
     * (slow) handshake; only the insert-or-reuse decision is locked. The first builder wins; losers
     * discard their freshly built entry (its socketFd is closed by ShmFd's destructor) and reuse the
     * existing one. If the connector throws, the in-flight entry is not in the map (no half-built
     * residue) and its socketFd is closed by ~ShmFd (review fix #1/#6).
     * @param[in] workerAddr Target worker address.
     * @return Pointer to the pooled entry (valid while the caller holds the pool and the entry is not
     *         released), or nullptr on connector failure. The entry is NOT copied out (ShmEntry is
     *         move-only); callers must use it in place (review fix #2/#9).
     */
    const ShmEntry *GetOrCreateShm(const HostPort &workerAddr);

    /**
     * @brief Release one worker's shm resources: close socketFd + drop the entry (UC2/UC3/UC8).
     * Idempotent; no-op if the worker was never pooled.
     * @param[in] workerAddr Worker whose entry to release.
     * @note This PR does not register the socketFd with the worker's epoll loop (SOCKET_HEARTBEAT is
     *       not activated on the AddClient path yet — see worker_oc_server.cpp AddClient comment), so
     *       closing directly is safe. When the new flow wires SOCKET_HEARTBEAT, the fd must be
     *       DelFdEvent'd from the worker's heartbeatEventLoop_ before close to avoid epoll use-after-
     *       close (review fix #7).
     */
    void ReleaseShm(const HostPort &workerAddr);

    /**
     * @brief Release every entry (UC10: client Shutdown). Closes each socketFd and clears the map.
     */
    void ReleaseAll();

    /**
     * @brief Read-only lookup (shared access path). Returns a pointer to the pooled entry (valid while
     * the caller holds the pool and the entry is not released), or nullptr if the worker is not pooled
     * or its entry is not yet valid. The entry is used in place, not copied (review fix #2/#9).
     */
    const ShmEntry *GetShmEntry(const HostPort &workerAddr) const;

    /**
     * @brief Number of pooled entries (for diagnostics/tests).
     */
    size_t Size() const;

private:
    std::string sdkHostId_;
    ShmConnector connector_;
    EntryMap entries_;
};
}  // namespace client
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_TRANSPORT_SHM_CONNECTION_POOL_H