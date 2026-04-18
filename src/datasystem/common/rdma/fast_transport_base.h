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
 * Description: Base for Fast Transport.
 */

#include <cstdint>

#include "datasystem/utils/status.h"

#ifndef DATASYSTEM_COMMON_FAST_TRANSPORT_BASE_H
#define DATASYSTEM_COMMON_FAST_TRANSPORT_BASE_H
namespace datasystem {
/**
 * @brief Register fast transport memory (as segment).
 * @param[in] segAddress Starting address of the segment.
 * @param[in] segSize Size of the segment.
 * @return Status of the call.
 */
Status RegisterFastTransportMemory(void *segAddress, const uint64_t &segSize);

/**
 * @brief Register host side memory (shared memory) to NPU device as segment.
 * @param[in] data The memory address.
 * @param[in] dataSize The size of the memory segment.
 * @return Status of the call.
 */
Status RegisterHostMemory(void *segAddress, const uint64_t &segSize);

/**
 * @brief Check if fast transport is enabled.
 * @return True if fast transport logic is compiled and the flag is set, else false.
 */
bool IsFastTransportEnabled();

/**
 * @brief Whether remote H2D is enabled according to FLAGS_enable_remote_h2d.
 * @return true if remote H2D is enabled.
 */
bool IsRemoteH2DEnabled();

/**
 * @brief Check if URMA is enabled.
 * @return True if URMA logic is compiled and the flag is set, else false.
 */
bool IsUrmaEnabled();

/**
 * @brief Check if Ucp is enabled.
 * @return True if Ucp logic is compiled and the flag is set, else false.
 */
bool IsUcpEnabled();

/**
 * @brief Check we should register whole arena upfront
 * @return True if flag is set, else false
 */
bool IsRegisterWholeArenaEnabled();

/**
 * @brief Check whether UB numa affinity optimization is enabled.
 * @return True if the feature flag, URMA and whole-arena registration are all enabled.
 */
bool IsUbNumaAffinityEnabled();

/**
 * @brief Check if the whole arena needs to be registered.
 * @return True if the whole arena needs to be registered, else false.
 */
bool NeedRegisterWholeArena();

/**
 * @brief Wait for the event of urma_write/ucp_put_nbx finish.
 * @param[in] keys The request ids to wait for events.
 * @param[in] remainingTime The callback to calculate remaining time.
 * @param[in] errorHandler The error handling callback.
 * @return Status of the call.
 */
Status WaitFastTransportEvent(std::vector<uint64_t> &keys, std::function<int64_t(void)> remainingTime,
                              std::function<Status(Status &)> errorHandler);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_FAST_TRANSPORT_BASE_H
