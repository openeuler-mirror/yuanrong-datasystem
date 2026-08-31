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
 * Description: Shared hash-ring token derivation contract.
 */
#ifndef DATASYSTEM_COMMON_UTIL_HASH_RING_TOKEN_H
#define DATASYSTEM_COMMON_UTIL_HASH_RING_TOKEN_H

#include <cstdint>
#include <string>
#include <vector>

namespace datasystem {

inline constexpr uint32_t MAX_HASH_RING_TOKEN_SEEDS = 10'000;
inline constexpr uint32_t MAX_HASH_RING_TOKENS_PER_MEMBER = 4'096;

uint32_t MakeHashRingToken(const std::string &address, uint32_t index, uint32_t seed);

void MakeHashRingTokens(const std::string &address, const std::vector<uint32_t> &seeds,
                        std::vector<uint32_t> &tokens);

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_UTIL_HASH_RING_TOKEN_H
