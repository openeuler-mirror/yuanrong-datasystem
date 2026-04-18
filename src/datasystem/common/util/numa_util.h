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
 * Description: NUMA utility.
 */
#ifndef DATASYSTEM_COMMON_UTIL_NUMA_UTIL_H
#define DATASYSTEM_COMMON_UTIL_NUMA_UTIL_H

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
const uint8_t INVALID_NUMA_ID = std::numeric_limits<uint8_t>::max();
const uint8_t INVALID_CHIP_ID = std::numeric_limits<uint8_t>::max();

/**
 * @brief Parse Linux cpulist format (e.g., "0,2,4-6") into sorted CPU ids.
 * @param[in] cpuListRaw Raw cpulist string.
 * @param[out] cpus Parsed CPU ids.
 * @return True if parsing succeeds and at least one CPU is parsed; otherwise false.
 */
bool ParseCpuList(const std::string &cpuListRaw, std::vector<int> &cpus);

/**
 * @brief Distribute a memory range across all discovered NUMA nodes.
 * @param[in] pointer Memory range start address.
 * @param[in] size Memory range size in bytes.
 * @return Status of this call.
 */
Status DistributeMemoryAcrossAllNumaNodes(void *pointer, size_t size);

/**
 * @brief Distribute a memory range across NUMA nodes that intersect current process CPU affinity.
 * @param[in] pointer Memory range start address.
 * @param[in] size Memory range size in bytes.
 * @return Status of this call.
 */
Status DistributeMemoryAcrossAffinityNumaNodes(void *pointer, size_t size);

/**
 * @brief Convert NUMA id to chip id used by transport logic.
 * @param[in] numaId NUMA id.
 * @param[in] numaCount NUMA node count.
 * @return Chip id, or INVALID_CHIP_ID for invalid NUMA id.
 */
uint8_t NumaIdToChipId(uint8_t numaId, size_t numaCount = 0);
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_NUMA_UTIL_H
