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
 * Description: Memory mmap instance.
 */

#include <atomic>
#include <cstdint>

#include "datasystem/common/shared_memory/mmap/base_mmap.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace memory {
class MemMmap : public BaseMmap {
public:
    MemMmap() = default;

    ~MemMmap() = default;

    /**
     * @brief Initialize mmap instance.
     * @param[in] size Mmap max size.
     * @param[in] populate Indicate whether pre-populate or not.
     * @param[in] hugepage Indicate whether enable hugepage or not.
     * @return K_OK on success; the error code otherwise.
     */
    Status Initialize(uint64_t size, bool populate = false, bool hugepage = false) override;

    /**
     * @brief Commits any physical resources to back pages at given addr and size at offset bytes,
              extending for length on behalf of arena, returning false upon success.
     * @param[in] addr The commit address.
     * @param[in] offset Offset bytes of given addr.
     * @param[in] length extending length.
     * @return True indicates commit failed; false means success.
     */
    bool Commit(void *addr, size_t offset, size_t length) override;

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
    bool Decommit(void *addr, size_t offset, size_t length) override;

private:
    bool InRange(void *pointer, ptrdiff_t &offset);
};
}  // namespace memory
}  // namespace datasystem