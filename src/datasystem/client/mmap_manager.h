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
#include <shared_mutex>
#include <string>
#include <vector>

#include "datasystem/client/client_worker_common_api.h"
#include "datasystem/client/mmap_table.h"
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
    explicit MmapManager(std::shared_ptr<ClientWorkerCommonApi> clientWorker);

    ~MmapManager();

    /**
     * @brief Loop the input share memory unit and mmap the fd if it was not mmapped in the client.
     * @param[in] tenantId for producer consumer get clientfd by tenantId.
     * @param[in] unit The input share memory unit.
     * @return Status of the call.
     */
    Status LookupUnitsAndMmapFd(const std::string &tenantId, std::shared_ptr<ShmUnitInfo> &unit);

    /**
     * @brief Loop the input share memory unit and mmap the fd if it was not mmapped in the client.
     * @param[in] tenantId for producer consumer get clientfd by tenantId
     * @param[in] units The input share memory unit.
     * @return Status of the call.
     */
    Status LookupUnitsAndMmapFd(const std::string &tenantId, std::vector<std::shared_ptr<ShmUnitInfo>> &units);

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
    std::shared_ptr<MmapTableEntry> GetMmapEntryByFd(int fd);

    /**
     * @brief Clear the invalid fds.
     * @param[in] fds The worker share memory file descriptor.
     */
    void ClearExpiredFds(const std::vector<int64_t> &fds);

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
    std::shared_ptr<ClientWorkerCommonApi> clientWorker_;
    std::unique_ptr<MmapTable> mmapTable_;
    mutable std::shared_timed_mutex mutex_;  // protect mmapTable_.
};
}  // namespace client
}  // namespace datasystem
#endif