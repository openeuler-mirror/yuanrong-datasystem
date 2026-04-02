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
 * Description: Client mmap table entry management.
 */
#ifndef DATASYSTEM_CLIENT_MMAP_EMBEDDED_MMAP_TABLE_ENTRY_H
#define DATASYSTEM_CLIENT_MMAP_EMBEDDED_MMAP_TABLE_ENTRY_H

#include <atomic>
#include <cstdint>

#include "datasystem/client/mmap/immap_table_entry.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class EmbeddedMmapTableEntry : public IMmapTableEntry {
public:
    EmbeddedMmapTableEntry(int fd, size_t mmapSize) : IMmapTableEntry(fd, mmapSize){};
    ~EmbeddedMmapTableEntry() = default;

    /**
     * @brief Mmap the client fd.
     * @param[in] enableHugeTlb huge_tlb switch
     * @return Status of the call.
     */
    Status Init(bool enableHugeTlb, const std::string &tenantId) override;

private:
    friend class IMmapTable;
};
}  // namespace client
}  // namespace datasystem
#endif
