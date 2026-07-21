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
 * Description: Client mmap table management.
 */
#ifndef DATASYSTEM_CLIENT_MMAP_IMMAP_TABLE_H
#define DATASYSTEM_CLIENT_MMAP_IMMAP_TABLE_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/client/mmap/immap_table_entry.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class IMmapTable {
public:
    IMmapTable() = delete;

    ~IMmapTable() = default;

    explicit IMmapTable(bool enableHugeTlb);
    /**
     * @brief Look up and mmap the share memory file descriptor.
     * @param[in] clientFd The client share memory file descriptor.
     * @param[in] workerFd The worker share memory file descriptor.
     * @param[in] mmapSize The share memory file mmap size.
     * @param[in] tenantId The tenant ID.
     * @param[in] clientId The client ID associated with the mmap operation.
     * @return Status of the call.
     */
    virtual Status MmapAndStoreFd(const int &clientFd, const int &workerFd, const uint64_t &mmapSize,
                                  const std::string &tenantId, const std::string &clientId = "") = 0;

    /**
     * @brief Look up the mmapped share memory file.
     * @param[in] workerFd The worker share memory file descriptor.
     * @param[out] pointer The pointer of the share memory mmap.
     * @return Status of the call.
     */
    Status LookupFdPointer(const int &workerFd, uint8_t **pointer);

    /**
     * @brief Find the fd whether is existed.
     * @param[in] workerFd The worker share memory file descriptor.
     * @return True for existed.
     */
    bool FindFd(const int &workerFd);

    /**
     * @brief Clear mmapTable.
     */
    void Clear();

    /**
     * @brief Invalid the current mmap table.
     */
    void CleanInvalidMmapTable();

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
     * @brief Associate a shm_id with the worker fd of an already-stored entry. The new
     * (enableLocalCache=false) multi-worker flow uses shm_id as the stable per-worker key so that
     * expired-fd reclaim and full release can be scoped per worker without relying on fd-number
     * space assumptions (review fix #3). Old flow entries simply never call this.
     * @param[in] workerFd The worker fd whose entry should carry the shm_id.
     * @param[in] shmId The worker-assigned shm region id.
     */
    void AssociateShmId(int workerFd, const std::string &shmId);

    /**
     * @brief Resolve the worker fd backing a shm_id, or -1 if none.
     */
    int GetWorkerFdByShmId(const std::string &shmId);

    /**
     * @brief Reclaim only the fds that belong to the entry carrying shm_id. Looks up the entry via
     * shm_id (not by assuming the fd number space) so worker A's expired fds never reclaim worker B's
     * entry (UC6 / review fix #3).
     * @param[in] shmId The owning shm_id.
     * @param[in] fds The expired fd list reported by the worker (filtered to this shm_id's entry).
     */
    void ClearExpiredByShmId(const std::string &shmId, const std::vector<int64_t> &fds);

    /**
     * @brief Drop the entry carrying shm_id and remove the shm_id mapping (full per-worker release).
     * Single critical section over mutex_ so concurrent AssociateShmId/ClearByShmId on the same shm_id
     * cannot race (review fix #4).
     */
    void ClearByShmId(const std::string &shmId);

    /*
     * @brief Get all worker file descriptors in the mmap table.
     * @return Worker file descriptors currently stored in the mmap table.
     */
    std::vector<int64_t> GetFds();

protected:
    // Protects 'mmapTable_' and 'shmIdToWorkerFd_'.
    std::shared_timed_mutex mutex_;

    // The mmap fd table. The key is worker fd, value is mmap entry.
    std::unordered_map<int, std::shared_ptr<IMmapTableEntry>> mmapTable_;
    // Reverse index for the new flow: shm_id -> worker fd. Empty for enableLocalCache=true.
    std::unordered_map<std::string, int> shmIdToWorkerFd_;
    //   huge_tlb switch
    bool enableHugeTlb_;
};
}  // namespace client
}  // namespace datasystem
#endif
