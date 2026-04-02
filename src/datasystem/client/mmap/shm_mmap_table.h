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
#ifndef DATASYSTEM_CLIENT_MMAP_SHM_MMAP_TABLE_H
#define DATASYSTEM_CLIENT_MMAP_SHM_MMAP_TABLE_H

#include <memory>
#include <shared_mutex>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_map>

#include "datasystem/common/log/log.h"
#include "datasystem/client/mmap/immap_table.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class ShmMmapTable : public IMmapTable {
public:
    ShmMmapTable() = delete;

    ~ShmMmapTable() = default;

    ShmMmapTable(bool enableHugeTlb) : IMmapTable(enableHugeTlb) {};

    /**
     * @brief Look up and mmap the share memory file descriptor.
     * @param[in] clientFd The client share memory file descriptor.
     * @param[in] workerFd The worker share memory file descriptor.
     * @param[in] mmapSize The share memory file mmap size.
     * @return Status of the call.
     */
    Status MmapAndStoreFd(const int &clientFd, const int &workerFd, const uint64_t &mmapSize,
                          const std::string &tenantId) override;
};
}  // namespace client
}  // namespace datasystem
#endif
