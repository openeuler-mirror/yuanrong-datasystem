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
 * Description: base mmap.
 */
#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_BASE_MMAP_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_BASE_MMAP_H

#include <atomic>
#include <cstdint>
#include <fcntl.h>

#include "datasystem/common/shared_memory/mmap/immap.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"

// In EulerOS, F_SEAL_SHRINK and F_SEAL_GROW are defined in "linux/fcntl.h", but
// it conflicts with "fcntl.h", so we define them by ourselves in this scenario.
#ifndef F_SEAL_SEAL
#define F_SEAL_SEAL 0x0001
#endif

#ifndef F_SEAL_SHRINK
#define F_SEAL_SHRINK 0x0002
#endif

#ifndef F_SEAL_GROW
#define F_SEAL_GROW 0x0004
#endif

#ifndef F_ADD_SEALS
#define F_ADD_SEALS 1033
#endif

namespace datasystem {
namespace memory {
class BaseMmap : public IMmap {
public:
    BaseMmap() = default;

    virtual ~BaseMmap();

    /**
     * @brief Destroy mmap instance.
     */
    void Destroy() override;

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
    void *Allocate(size_t size, size_t alignment, bool *zero, bool *commit) override;

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
    virtual bool Decommit(void *addr, size_t offset, size_t length) override;

    /**
     * @brief Return mmap file descriptor.
     * @return File descriptor.
     */
    int Fd() const override
    {
        return fd_;
    }

    /**
     * @brief Return the mmap pointer.
     * @return The mmap pointer
     */
    void *Pointer() const override
    {
        return pointer_;
    }

    /**
     * @brief Get allocation information via pointer.
     * @param[in] pointer Mmap address.
     * @return Allocation information if success; nullptr otherwise.
     */
    Optional<Allocation> GetAllocation(void *pointer) const override;

protected:
    /**
     * @brief Sets up memory mapping for a file.
     * @param[in] size The mmap size.
     * @param[in] flags The mmap flags.
     * @param[in] isSeal Is seal the fd or not.
     * @return Status K_OK on success; the error code otherwise.
     */
    Status SetupFileMapping(size_t size, int flags, bool isSeal);

    // Shared memory fd.
    int fd_;

    // Head pointer.
    void *pointer_;

    // Mmap size.
    uint64_t mmapSize_;

    // Current pointer.
    std::atomic<uintptr_t> curr_;

    // Tail pointer.
    uintptr_t tail_;

    // The mmap type.
    std::string type_;
};
}  // namespace memory
}  // namespace datasystem

#endif