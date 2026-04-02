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
#ifndef DATASYSTEM_CLIENT_MMAP_IMMAP_TABLE_ENTRY_H
#define DATASYSTEM_CLIENT_MMAP_IMMAP_TABLE_ENTRY_H

#include <atomic>
#include <cstdint>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class IMmapTableEntry {
public:
    explicit IMmapTableEntry(int fd, size_t mmapSize) : fd_(fd), size_(mmapSize), pointer_(nullptr) {};
    ~IMmapTableEntry() = default;

    /**
     * @brief Mmap the client fd.
     * @param[in] enableHugeTlb huge_tlb switch
     * @return Status of the call.
     */
    virtual Status Init(bool enableHugeTlb, const std::string &tenantId) = 0;

    /**
     * @brief Get the fd pointer.
     * @return The fd pointer.
     */
    const uint8_t *Pointer()
    {
        return pointer_;
    }

protected:
    friend class IMmapTable;

    int fd_;
    size_t size_;
    uint8_t *pointer_;
};
}  // namespace client
}  // namespace datasystem
#endif
