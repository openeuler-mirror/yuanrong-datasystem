/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: mmap interface.
 */

#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_MMAP_IMMAP_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_MMAP_IMMAP_H

#include "datasystem/common/shared_memory/mmap/allocation.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace memory {
class IMmap {
public:
    IMmap() = default;

    virtual ~IMmap() = default;

    /**
     * @brief Initialize mmap instance.
     * @param[in] size Mmap max size.
     * @param[in] populate Indicate whether pre-populate or not.
     * @param[in] hugepage Indicate whether enable hugepage or not.
     * @return K_OK on success; the error code otherwise.
     */
    virtual Status Initialize(uint64_t size, bool populate = false, bool hugepage = false) = 0;

    /**
     * @brief Destroy mmap instance.
     */
    virtual void Destroy() = 0;

    /**
     * @brief Allocate is invoked whenever the arena needs additional memory from the OS,
     *        e.g. when all local mapped memory are in use, and we need more memory to satisfy
     *        the current request (which is typical done through mmap).
     * @param[in] size Always a multiple of the page size in bytes.
     * @param[in] alignment Always a power of two at least as large as the page size.
     * @param[out] zero Indicate whether the extent is zeroed.
     * @param[out] commit Indicate whether the extent is committed.
     * @return A pointer to size bytes of mapped memory on behalf of arena.
     */
    virtual void *Allocate(size_t size, size_t alignment, bool *zero, bool *commit) = 0;

    /**
     * @brief Commits any physical resources to back pages at given addr and size at offset bytes,
              extending for length on behalf of arena, returning false upon success.
     * @param[in] addr The commit address.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @return True indicates commit failed; false means success.
     */
    virtual bool Commit(void *addr, size_t offset, size_t length) = 0;

    /**
     * @brief Decommits any physical resources that is backing pages at given addr and size at offset bytes,
     *        extending for length on behalf of arena arena_ind. Return false upon success, in which case
     *        the pages will be committed via the extent commit function before being reused. If the function
     *        returns true, this indicates opt-out from decommit; the resources remains committed and available
     *        for future use, in which case it will be automatically retained for later reuse.
     * @param[in] addr The decommit address.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @return True indicates remains committed and available for future use; false means decommit success.
     */
    virtual bool Decommit(void *addr, size_t offset, size_t length) = 0;

    /**
     * @brief Return mmap file descriptor.
     * @return File descriptor.
     */
    virtual int Fd() const = 0;

    /**
     * @brief Return the mmap pointer.
     * @return The mmap pointer
     */
    virtual void *Pointer() const = 0;

    /**
     * @brief Get allocation information via pointer.
     * @param[in] pointer Mmap address.
     * @return Allocation information if success; nullptr otherwise.
     */
    virtual Optional<Allocation> GetAllocation(void *pointer) const = 0;
};
}  // namespace memory
}  // namespace datasystem

#endif