/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Client mmap management.
 */
#ifndef DATASYSTEM_CLIENT_MMAP_MANAGER_H
#define DATASYSTEM_CLIENT_MMAP_MANAGER_H

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include "datasystem/client/client_worker_common_api.h"
#include "datasystem/client/mmap/immap_table.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
/**
 * @brief Manage the mmapTable in the client side, which is global one instance in a process.
 */
class MmapManager {
public:
    explicit MmapManager(std::shared_ptr<IClientWorkerCommonApi> clientWorker, bool enableEmbeddedClient);

    ~MmapManager();

    /**
     * @brief Loop the input share memory unit and mmap the fd if it was not mmapped in the client.
     * @param[in] tenantId for stream producer consumer get clientfd by tenantId.
     * @param[in] unit The input share memory unit.
     * @return Status of the call.
     */
    Status LookupUnitsAndMmapFd(const std::string &tenantId, const std::shared_ptr<ShmUnitInfo> &unit);

    /**
     * @brief Loop the input share memory unit and mmap the fd if it was not mmapped in the client.
     * @param[in] tenantId for stream producer consumer get clientfd by tenantId
     * @param[in] units The input share memory unit.
     * @return Status of the call.
     */
    Status LookupUnitsAndMmapFds(const std::string &tenantId, std::vector<std::shared_ptr<ShmUnitInfo>> &units);

    /**
     * @brief Get memory mapped data by stored file descriptor.
     * @param[in] storeFd The file descriptor of memory mapped data.
     * @return data address in memory.
     */
    uint8_t *LookupMmappedFile(const int storeFd);

    /**
     * @brief Get mmap entry by worker fd.
     * @param[in] fd Worker fd.
     * @return Mmap entry.
     */
    std::shared_ptr<IMmapTableEntry> GetMmapEntryByFd(int fd);

    /**
     * @brief Clear the invalid fds.
     * @param[in] fds The worker share memory file descriptor.
     */
    void ClearExpiredFds(const std::vector<int64_t> &fds);

    /**
     * @brief Associate a shm_id with the worker fd of an already-mmap'd entry, enabling per-worker
     * scoped reclaim in the new (enableLocalCache=false) flow. Delegates to IMmapTable which holds the
     * shm_id -> worker fd reverse index under the same lock as the mmap table (no TOCTOU, no fd-number
     * space assumption; review fix #3/#4/#5).
     * @param[in] workerFd The worker fd (the shm DATA fd that worker-side GetAllExpiredFds returns,
     *                     same number space as the fds in ClearExpiredByShmId).
     * @param[in] shmId Worker-assigned unique shm region id (worker_uuid-prefixed).
     */
    void AssociateShmId(int workerFd, const std::string &shmId);

    /**
     * @brief Resolve the worker fd backing a shm_id, or -1 if none. Delegates to IMmapTable.
     */
    int GetWorkerFdByShmId(const std::string &shmId) const;

    /**
     * @brief Reclaim expired fds scoped to a shm_id. The owning worker fd is resolved via the
     * shm_id reverse index (not by assuming the fd number space), so worker A's expired fds never
     * touch worker B's mmap entry (UC6 / review fix #3). Delegates to IMmapTable.
     * @param[in] shmId The shm_id whose expired fds should be reclaimed.
     * @param[in] fds The expired fd list reported by the worker (filtered to this shm_id's entry).
     */
    void ClearExpiredByShmId(const std::string &shmId, const std::vector<int64_t> &fds);

    /**
     * @brief Drop all mmap state for a shm_id: clear the underlying fd entry from the mmap table and
     * remove the shm_id -> fd mapping. Used when the client stops routing to a worker (topology change
     * / RPC failure) to release that worker's shm resources wholesale (UC3/UC10). Delegates to IMmapTable.
     * @param[in] shmId The shm_id to release.
     */
    void ClearByShmId(const std::string &shmId);

    /**
     * @brief Clear mmapTable.
     */
    void Clear();

    /**
     * @brief Invalid the current mmap table.
     */
    void CleanInvalidMmapTable();

private:
    // The instance for client communication with the worker.
    std::shared_ptr<IClientWorkerCommonApi> clientWorker_;
    std::unique_ptr<IMmapTable> mmapTable_;
    mutable std::shared_timed_mutex mutex_;  // protect mmapTable_.
    // The fd transfer channel behind GetClientFd() is single-consumer on each client-worker connection.
    std::mutex fdTransferMutex_;
    bool enableEmbeddedClient_;
};
}  // namespace client
}  // namespace datasystem
#endif
