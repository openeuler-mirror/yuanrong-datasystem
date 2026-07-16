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

#ifndef DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_MEMFD_RESOLVER_H
#define DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_MEMFD_RESOLVER_H

#include <cstdint>
#include <string>

namespace datasystem {
namespace urma_mock {
/**
 * @brief Metadata for the memfd mapping that covers a virtual address range.
 */
struct MemfdMapping {
    uint64_t inode = 0;
    uint64_t offset = 0;
};

/**
 * @brief Look up the memfd fd backing a virtual address range.
 * The resolver matches /proc/self/maps and /proc/self/fd entries by inode so same-name memfds can be disambiguated.
 *
 * @param[in] va Start address of the mapped range.
 * @param[in] len Range length in bytes.
 * @param[in] memfdName Expected memfd name.
 * @return A dup'd fd on success; -1 if no matching memfd is found. The caller owns and must close the returned fd.
 */
int LookupMemfdFd(uint64_t va, uint64_t len, const std::string &memfdName);

/**
 * @brief Look up the memfd fd and file offset backing a virtual address range.
 * @param[in] va Start address of the mapped range.
 * @param[in] len Range length in bytes.
 * @param[in] memfdName Expected memfd name.
 * @param[out] mapping Matching mapping metadata.
 * @return A dup'd fd on success; -1 if no matching memfd is found. The caller owns and must close the returned fd.
 */
int LookupMemfdFd(uint64_t va, uint64_t len, const std::string &memfdName, MemfdMapping *mapping);

}  // namespace urma_mock
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_URMA_MOCK_SEGMENT_MEMFD_RESOLVER_H
