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
 * Description: Define Jemalloc allocator class, Jemalloc is used to manage
 *              memory allocate and free.
 */
#ifndef DATASYSTEM_COMMON_SHARED_MEMORY_JEMALLOC_H
#define DATASYSTEM_COMMON_SHARED_MEMORY_JEMALLOC_H

#include <functional>
#include <memory>
#include <unordered_map>

#include "jemalloc/jemalloc.h"

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"

typedef struct extent_hooks_s extent_hooks_t;

typedef void *(AllocHook)(size_t, size_t, unsigned, bool *, bool *);

typedef void(DestroyHook)(void *, size_t, bool, unsigned);

typedef bool(CommitHook)(bool commit, void *, size_t, size_t, size_t, unsigned);

namespace datasystem {
namespace memory {
class Jemalloc {
public:
    Jemalloc() = default;

    ~Jemalloc() = default;

    /**
     * @brief Init the jemalloc hook.
     * @param[in] alloc Allocate extent hook.
     * @param[in] destroy Destroy extent hook.
     * @param[in] commit commit extent hook.
     * @param[in] uncommit uncommit extent hook.
     */
    static void Init(::AllocHook *alloc, ::DestroyHook *destroy, ::CommitHook *commit);

    /**
     * @brief Create new arena.
     * @param[in] decayMs Purge decay ms.
     * @param[out] arenaInd New created arena index.
     * @param[out] handler Arena handler you need to be hold.
     * @return K_OK if success, the error otherwise.
     */
    static Status CreateArena(ssize_t decayMs, unsigned &arenaInd, void *&handler);

    /**
     * @brief Destroy arena.
     * @param[in] arenaInd Arena index.
     * @return K_OK if success, the error otherwise.
     */
    static Status DestroyArena(unsigned arenaInd);

    /**
     * @brief Destroy handler in arena.
     * @param[in] handler Arena handler.
     */
    static void DestroyHandler(void *handler);

    /**
     * @brief Allocate the memory, just like malloc.
     * @param[in] arenaInd Arena index.
     * @param[in, out] bytes Read needed memory size in bytes and write actual allocated size.
     * @param[out] pointer Allocated memory head pointer.
     * @return K_OK if success, the error otherwise.
     */
    static Status Allocate(unsigned arenaInd, uint64_t &bytes, void *&pointer);

    /**
     * @brief Free the allocated memory via pointer. If pointer is null, it will do nothing.
     * @param[in] arenaInd Arena index.
     * @param[in] pointer Allocated memory pointer.
     */
    static void Free(unsigned arenaInd, void *pointer);

private:
    /**
     * @brief Get allocated size via pointer.
     * @param[in] pointer Allocated memory head pointer.
     * @param[in] arenaInd Arena index.
     * @return Allocated size.
     */
    static size_t GetAllocatedSize(void *pointer, unsigned arenaInd);

    /**
     * @brief The alloc hook is invoked whenever the arena needs additional memory from the OS, e.g. when
     *        all local mapped memory are in use, and we need more memory to satisfy the current request
     *        (which is typical done through mmap).
     * @param[in] extentHooks Extent hook struct.
     * @param[in] newAddr New address.
     * @param[in] size Always a multiple of the page size.
     * @param[in] alignment Always a power of two at least as large as the page size.
     * @param[out] zero Indicate whether the extent is zeroed.
     * @param[in/out] commit Indicate whether the extent is committed.
     * @param[in] arenaInd Arena index.
     * @return A pointer to size bytes of mapped memory on behalf of arena.
     */
    static void *AllocHook(extent_hooks_t *extentHooks, void *newAddr, size_t size, size_t alignment, bool *zero,
                           bool *commit, unsigned arenaInd);

    /**
     * @brief Destroys an extent at given addr and size with committed/decommited memory as indicated, on behalf
     *        of arena arenaInd. This function may be called to destroy retained extents during arena destruction.
     * @param[in] extentHooks Extent hook struct.
     * @param[in] addr Deallocate address.
     * @param[in] size Deallocate size.
     * @param[in] committed Commit state.
     * @param[in] arenaInd Arena index.
     */
    static void DestroyHook(extent_hooks_t *extentHooks, void *addr, size_t size, bool committed, unsigned arenaInd);

    /**
     * @brief Commits zeroed physical memory to back pages within an extent at given addr and size at offset bytes,
     *        extending for length on behalf of arena arena_ind, returning false upon success. Committed memory may be
     *        committed in absolute terms as on a system that does not overcommit, or in implicit terms as on a system
     *        that overcommits and satisfies physical memory needs on demand via soft page faults. If the function
     *        returns true, this indicates insufficient physical memory to satisfy the request
     * @param[in] extentHooks Extent hook struct.
     * @param[in] addr The commit address.
     * @param[in] size Address size.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @param[in] arenaInd Arena index.
     * @return True indicates insufficient physical memory to satisfy the request; false means commit success.
     */
    static bool CommitHook(extent_hooks_t *extentHooks, void *addr, size_t size, size_t offset, size_t length,
                           unsigned arenaInd);

    /**
     * @brief Decommits any physical memory that is backing pages within an extent at given addr and size at offset
     *        bytes, extending for length on behalf of arena arena_ind. Return false upon success, in which case
     *        the pages will be committed via the extent commit function before being reused. If the function returns
     *        true, this indicates opt-out from decommit; the memory remains committed and available for future use,
     *        in which case it will be automatically retained for later reuse.
     * @param[in] extentHooks Extent hook struct.
     * @param[in] addr Lazy purge address.
     * @param[in] size Address size.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @param[in] arenaInd Arena index.
     * @return True indicates remains committed and available for future use; false means decommit success.
     */
    static bool DecommitHook(extent_hooks_t *extentHooks, void *addr, size_t size, size_t offset, size_t length,
                             unsigned arenaInd);

    /**
     * @brief Discards physical pages within the virtual memory mapping associated with an extent at given addr and
     *        size at offset bytes, extending for length on behalf of arena arenaInd. A lazy extent purge function
     *        (e.g. implemented via madvise(...MADV_FREE)) can delay purging indefinitely and leave the pages within
     *        the purged virtual memory range in an indeterminite state.
     * @param[in] extentHooks Extent hook struct.
     * @param[in] addr Lazy purge address.
     * @param[in] size Address size.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @param[in] arenaInd Arena index.
     * @return True indicates failure to purge.
     */
    static bool PurgeLazyHook(extent_hooks_t *extentHooks, void *addr, size_t size, size_t offset, size_t length,
                              unsigned arenaInd);

    /**
     * @brief Discards physical pages within the virtual memory mapping associated with an extent at given addr and
     *        size at offset bytes, extending for length on behalf of arena arenaInd. Forced extent purge function
     *        immediately purges, and the pages within the virtual memory range will be zero-filled the next time
     *        they are accessed.
     * @param[in] extentHooks Extent hook struct.
     * @param[in] addr Lazy purge address.
     * @param[in] size Address size.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @param[in] arenaInd Arena index.
     * @return True indicates failure to purge.
     */
    static bool PurgeForcedHook(extent_hooks_t *extentHooks, void *addr, size_t size, size_t offset, size_t length,
                                unsigned arenaInd);

    /**
     * @brief Get DataSystem [s,m,d]allocx flag
     * @param [in] arenaInd arena index
     * @return flag
     * */
    static unsigned int GetAllocxFlags(unsigned int arenaInd);

    static ::AllocHook *alloc_;
    static ::DestroyHook *destroy_;
    static ::CommitHook *commit_;
};
}  // namespace memory
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_SHARED_MEMORY_JEMALLOC_H